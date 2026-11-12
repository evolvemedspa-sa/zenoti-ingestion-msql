"""Load product-detail rows, optionally inside the three-table transaction."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pyodbc

if __package__:
    from .db_helper import get_connection
else:
    from db_helper import get_connection


NY_TZ = ZoneInfo('America/New_York')
REFERENCE_BATCH_SIZE = 500
SQL_PARAMETER_BUDGET = 2000
SQL_VALUES_ROW_LIMIT = 1000


def quote_identifier(value):
    return '[' + value.replace(']', ']]') + ']'


def split_table_name(table):
    schema, name = 'dbo', table
    if '.' in table:
        schema, name = table.split('.', 1)
    schema, name = schema.strip(' []'), name.strip(' []')
    if not schema or not name:
        raise ValueError(f'Invalid detail table name: {table!r}')
    return schema, name


def describe_table(cursor, schema, name):
    """Return ordered column names, identity columns and ODBC type metadata."""
    cursor.execute('''
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
               c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
               COLUMNPROPERTY(
                   OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)),
                   c.COLUMN_NAME, 'IsIdentity'
               ) AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    ''', schema, name)
    rows = cursor.fetchall()
    if not rows:
        raise ValueError(f'Table not found: {quote_identifier(schema)}.{quote_identifier(name)}')
    columns = [row[0] for row in rows]
    identities = {row[0] for row in rows if row[5] == 1}
    metadata = {row[0]: {'type': row[1], 'length': row[2],
                         'precision': row[3], 'scale': row[4]} for row in rows}
    return columns, identities, metadata


def input_size_for(info):
    """Return only explicit ODBC sizes that have a valid positive precision."""
    data_type = (info['type'] or '').lower()
    if data_type in ('decimal', 'numeric'):
        precision = int(info['precision'] or 18)
        scale = int(info['scale'] if info['scale'] is not None else 2)
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            return None
        return (pyodbc.SQL_DECIMAL, precision, scale)
    if data_type in ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'):
        length = info['length']
        if length is None or length <= 0:
            return None
        return (pyodbc.SQL_WVARCHAR, length, 0)
    # Let ODBC infer sizes for numeric, date/time, and other types. Passing a
    # tuple with precision 0 (or a MAX text width of 0) can raise HY104.
    return None


def fetch_integer(cursor: pyodbc.Cursor) -> int:
    row = cursor.fetchone()
    if row is None:
        raise ValueError('SQL aggregate query returned no row.')
    return int(row[0])


def insert_rows(cursor, qualified, columns, data):
    """Insert rows without pyodbc's large array-binding allocation.

    ``fast_executemany`` sizes one array buffer from the destination schema. A
    varchar(max)/nvarchar(max) detail column can therefore raise ``MemoryError``
    even when the actual payload is only a few kilobytes. SQL Server accepts at
    most 2,100 parameters per statement, so use bounded multi-row VALUES batches
    instead. This is the same approach used by the order-summary loader.
    """
    if not data:
        return
    if not columns:
        raise ValueError('Detail insert has no writable columns.')
    if len(columns) > SQL_PARAMETER_BUDGET:
        raise ValueError('Detail insert has too many columns for SQL Server.')

    batch_size = min(SQL_VALUES_ROW_LIMIT, SQL_PARAMETER_BUDGET // len(columns))
    column_list = ','.join(quote_identifier(column) for column in columns)
    row_placeholders = '(' + ','.join('?' for _ in columns) + ')'
    for offset in range(0, len(data), batch_size):
        chunk = data[offset:offset + batch_size]
        values = ','.join(row_placeholders for _ in chunk)
        cursor.execute(
            f'INSERT INTO {qualified} ({column_list}) VALUES {values}',
            [value for row in chunk for value in row],
        )


def inspect_target(cursor, table, line_item_columns):
    """Check a detail table's writable fields without loading any rows."""
    schema, name = split_table_name(table)
    columns, identities, metadata = describe_table(cursor, schema, name)
    lookup = {column.lower(): column for column in columns if column not in identities}
    missing = [column for column in line_item_columns if column not in lookup]
    if missing:
        raise ValueError(f'{table} is missing writable column(s): {", ".join(missing)}')
    return lookup, metadata


def load_line_items(df, table, line_item_columns, *, replace=False,
                    connection=None, commit=True, references=None):
    """Replace only fetched orders; errors roll back rather than retrying an append.

    A supplied connection with commit=False lets the runner commit the summary
    and both detail tables together. References include fetched orders that no
    longer contain products, so obsolete detail rows can also be removed.
    """
    if connection is None and not commit:
        raise ValueError('A pending detail load requires an injected connection.')
    missing = [column for column in line_item_columns if column not in df.columns]
    if missing:
        raise ValueError(f'Detail rows are missing column(s): {", ".join(missing)}')
    item_refs = {str(value).strip() for value in df['ref_number'].tolist()}
    refs = sorted({str(value).strip() for value in references}
                  if references is not None else item_refs)
    if '' in item_refs or '' in refs:
        raise ValueError('Detail rows contain a blank order reference.')
    if not item_refs.issubset(refs):
        raise ValueError('Detail rows contain an order outside the fetched references.')

    schema, name = split_table_name(table)
    qualified = quote_identifier(schema) + '.' + quote_identifier(name)
    owns_connection = connection is None
    if connection is None:
        connection = get_connection()
    connection.timeout = 120
    cursor = connection.cursor()
    try:
        cursor.execute('SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;')
        lookup, metadata = inspect_target(cursor, qualified, line_item_columns)
        fields = list(line_item_columns)
        if 'date_inserted' in lookup:
            fields.append('date_inserted')
        columns = [lookup[field] for field in fields]
        out = df.copy()
        out['date_inserted'] = datetime.now(NY_TZ).strftime('%Y-%m-%d %H:%M:%S')
        out = out[fields]
        data = out.astype(object).where(out.notnull(), None).values.tolist()

        cursor.execute(f'SELECT COUNT_BIG(*) FROM {qualified} WITH (TABLOCKX, HOLDLOCK)')
        before = fetch_integer(cursor)
        deleted = 0
        if replace:
            ref_column = quote_identifier(lookup['ref_number'])
            for offset in range(0, len(refs), REFERENCE_BATCH_SIZE):
                batch = refs[offset:offset + REFERENCE_BATCH_SIZE]
                placeholders = ','.join('?' for _ in batch)
                predicate = f'{ref_column} IN ({placeholders})'
                cursor.execute(f'SELECT COUNT_BIG(*) FROM {qualified} WHERE {predicate}', batch)
                deleted += fetch_integer(cursor)
                cursor.execute(f'DELETE FROM {qualified} WHERE {predicate}', batch)

        insert_rows(cursor, qualified, columns, data)

        cursor.execute(f'SELECT COUNT_BIG(*) FROM {qualified}')
        after = fetch_integer(cursor)
        if after != before - deleted + len(data):
            raise ValueError(f'Detail row-count verification failed for {qualified}.')
        if commit:
            connection.commit()
        if commit:
            print(f'  SQL      {len(data):,} detail rows committed '
                  f'({deleted:,} replaced)', flush=True)
        return len(data)
    except BaseException:
        connection.rollback()
        raise
    finally:
        cursor.close()
        if owns_connection:
            connection.close()
