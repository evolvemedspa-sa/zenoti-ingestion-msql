"""CSV -> SQL Server table machinery, shared by the HC report loaders.

This is the subset of dbquery/import_aesthetix_opportunity_history.py that
import_hc_report.py uses, copied here so the zconnect image does not depend on
a folder that is gitignored: dbquery/ never reaches the Railway build context,
and every load died with ModuleNotFoundError.

Copied functions are unchanged; the CLI half of the original (argv parsing,
.env loading, table name, load/check entry points) is not copied, because none
of it is used here. Keep this in sync with the original if the coercion rules
change.
"""
import re
from datetime import date, datetime

import pandas as pd

# Audit columns (date_inserted, ingestion_timestamp) carry no source in any
# CSV. Ingestion scripts fill them; the loader reports the gap but does not
# fail on it. Present in the tuple whether or not the table has the column.
AUDIT_TIME_COLUMNS = ("date_inserted", "ingestion_timestamp")


def normalize_col(name):
    """
    Lowercase, collapse every run of non-alphanumeric characters to a single
    underscore, trim underscores, then drop any underscore that directly
    precedes a digit. Applied to BOTH the CSV headers and the introspected DB
    column names, so the match is symmetric and independent of column order,
    and a leading BOM on the first header collapses away too.
    """
    s = re.sub(r"[^0-9a-z]+", "_", (name or "").strip().lower()).strip("_")
    return re.sub(r"_+(?=\d)", "", s)


# SQL DATA_TYPE names (lowercased) whose CSV strings must be parsed to numbers
# before insert. Whether any of these is exercised depends on the live table.
NUMERIC_DB_TYPES = {
    "decimal", "numeric", "money", "smallmoney",
    "int", "bigint", "smallint", "tinyint", "float", "real",
}

# Tokens in a numeric/date cell that mean "no value" -> NULL (blank is already
# None by the time it reaches here; "-" is a common "not applicable" marker).
_NULL_TOKENS = {"", "-", "--", "n/a", "na"}


def _coerce_numeric(value, column):
    """
    Clean a numeric-looking string for a numeric DB column: strip "$"/","/
    whitespace, map ""/"-"/"n/a" -> None, otherwise float() it. ACCOUNTING
    notation "(1,000)" means -1000 and is parsed as a negative number. Raises
    ValueError naming the column on a non-blank value that is not a number, so
    bad data surfaces rather than being silently nulled.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in _NULL_TOKENS:
        return None
    cleaned = s.replace("$", "").replace(",", "").strip()
    if cleaned.lower() in _NULL_TOKENS:
        return None
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        inner = cleaned[1:-1].strip()
        if inner:
            cleaned, negative = inner, True
    try:
        f = float(cleaned)
    except ValueError:
        raise ValueError(f"column '{column}': cannot convert {value!r} to a number")
    return -f if negative else f


# SQL DATA_TYPE names that take a date/time. Values bound for these columns are
# parsed into real Python date/datetime objects, so pyodbc sends an unambiguous
# value (no reliance on the server's locale to parse "5/11/2020" as M/D vs D/M).
DATE_DB_TYPES = {"date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"}

# Tried in order; 4-digit-year forms first so a real year is never misread as a
# 2-digit year. The .%f form is what this export writes its *_at_utc columns as
# ("2024-03-21 16:00:27.000"). Yearless forms are intentionally ABSENT: "8-May"
# has no year and cannot become a valid DATE.
_DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%m/%d/%y",
)


def _coerce_date(value, column):
    """
    Parse a date string for a date/datetime DB column into a real datetime
    object. ""/"-"/"n/a" -> None; an already-parsed date/datetime passes
    through. Always returns a datetime -- a DATE column simply drops the time.
    Raises ValueError naming the column on a value that matches no accepted
    format, which includes month-day-only values like '8-May'.
    """
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value
    s = str(value).strip()
    if s.lower() in _NULL_TOKENS:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"column '{column}': cannot parse {value!r} as a date "
        f"(month-day-only values like '8-May' have no year and cannot go in a "
        f"DATE column -- store this column as text to keep such values verbatim)"
    )


def parse_csv(csv_path):
    """
    Read the CRM export into a list of dict rows keyed by NORMALIZED column name
    (so they align to the DB columns regardless of order). Blank cells become
    None; every other value is preserved verbatim.

    Read with encoding="utf-8-sig" so a UTF-8 BOM on the first header is
    stripped, and index_col=False so a trailing comma isn't read as an index.
    Fully-blank rows are skipped -- a row is dropped only when EVERY field is
    empty. Embedded quotes/commas/newlines inside a quoted field (raw_json,
    current_location here) are the csv parser's job and arrive intact.

    Returns (dict_rows, csv_norm_names) where csv_norm_names is the ordered list
    of normalized CSV headers, used for the mapping report.
    """
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, index_col=False,
                     encoding="utf-8-sig")

    def clean(v):
        if v is None:
            return None
        v = v.strip()
        return v if v != "" else None

    csv_norm_names = [normalize_col(c) for c in df.columns]
    dict_rows = []
    skipped_blank = 0
    # itertuples is an order of magnitude faster than iterrows over 100k+ rows.
    for row in df.itertuples(index=False, name=None):
        d = {}
        for i, norm in enumerate(csv_norm_names):
            if not norm:
                continue  # drop truly-empty spacer headers
            d[norm] = clean(row[i])
        if not any(v is not None for v in d.values()):
            skipped_blank += 1
            continue  # skip fully-blank rows (trailing spacer block)
        dict_rows.append(d)
    if skipped_blank:
        print(f"  skipped {skipped_blank:,} fully-blank row(s)")
    return dict_rows, csv_norm_names


def _split_table(qualified):
    """'dbo.NAME' / '[dbo].[NAME]' / 'NAME' -> ('dbo', 'NAME')."""
    schema, name = "dbo", qualified
    if "." in qualified:
        left, right = qualified.split(".", 1)
        schema = left.strip(" []")
        name = right.strip(" []")
    return schema, name.strip(" []")


def _table_columns(cursor, schema, name):
    """Return (columns, identity, types, lengths) for a table. `types` maps each
    column to its lowercased SQL DATA_TYPE so the loader knows which columns to
    coerce to numbers and which to dates; `lengths` maps a character column to
    its max length in characters (None for max/-1 or non-character types), so a
    value too long for its column can be reported instead of failing the load."""
    cursor.execute(
        """
        SELECT c.COLUMN_NAME,
               COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)),
                              c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
               c.DATA_TYPE,
               c.CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        WHERE c.TABLE_NAME = ? AND c.TABLE_SCHEMA = ?
        ORDER BY c.ORDINAL_POSITION
        """,
        (name, schema),
    )
    fetched = cursor.fetchall()
    columns = [r[0] for r in fetched]
    identity = {r[0] for r in fetched if r[1] == 1}
    types = {r[0]: (r[2] or "").lower() for r in fetched}
    lengths = {r[0]: r[3] for r in fetched}
    return columns, identity, types, lengths


def _to_tuples(dict_rows, insert_columns, col_types):
    """Align dict rows (keyed by normalized name) to the table's insert-column
    order, coercing each value by its target column's SQL type: numeric types via
    _coerce_numeric, date/datetime types via _coerce_date. Text columns pass
    through. A bad value raises ValueError naming row+column."""
    norm_targets = [normalize_col(c) for c in insert_columns]
    tuples = []
    for i, r in enumerate(dict_rows):
        vals = []
        for col, norm in zip(insert_columns, norm_targets):
            raw = r.get(norm)
            ctype = col_types.get(col) or ""
            try:
                if ctype in NUMERIC_DB_TYPES:
                    vals.append(_coerce_numeric(raw, col))
                elif ctype in DATE_DB_TYPES:
                    vals.append(_coerce_date(raw, col))
                else:
                    vals.append(raw)
            except ValueError as e:
                raise ValueError(f"row {i}: {e}")
        tuples.append(tuple(vals))
    return tuples


def _validate_types(dict_rows, insert_columns, col_types, lengths=None, sample=5):
    """Dry pre-flight: try to coerce every numeric/date column in every row the
    same way the load will, WITHOUT touching the DB, and check every text value
    against its column's max length. Returns {column: (fail_count, [samples])}
    for values that would fail -- so schema/data mismatches surface before any
    write instead of aborting the load mid-way."""
    lengths = lengths or {}
    bad = {}

    def flag(col, raw):
        cnt, samples = bad.get(col, (0, []))
        cnt += 1
        if len(samples) < sample and raw not in samples:
            samples.append(raw)
        bad[col] = (cnt, samples)

    for r in dict_rows:
        for col in insert_columns:
            raw = r.get(normalize_col(col))
            if raw is None:
                continue
            ctype = col_types.get(col) or ""
            if ctype in NUMERIC_DB_TYPES:
                try:
                    _coerce_numeric(raw, col)
                except ValueError:
                    flag(col, raw)
            elif ctype in DATE_DB_TYPES:
                try:
                    _coerce_date(raw, col)
                except ValueError:
                    flag(col, raw)
            else:
                limit = lengths.get(col)
                # -1 is varchar(max)/nvarchar(max): unlimited, nothing to check.
                if limit and limit > 0 and len(str(raw)) > limit:
                    flag(col, f"{raw!r} ({len(str(raw))} > {limit})")
    return bad


def _mapping_report(csv_norm_names, insert_columns):
    """
    Cross-check CSV headers against the table's insert columns.
    Returns (mapped, db_no_source, csv_unused) as lists of column names.
    """
    norm_targets = {normalize_col(c) for c in insert_columns}
    csv_set = {n for n in csv_norm_names if n}
    mapped = [c for c in insert_columns if normalize_col(c) in csv_set]
    db_no_source = [c for c in insert_columns if normalize_col(c) not in csv_set]
    csv_unused = [n for n in csv_norm_names if n and n not in norm_targets]
    return mapped, db_no_source, csv_unused


def _self_check():
    """Pin the rules a copy could get wrong: normalization, the two coercers,
    the mapping split, and CSV parsing."""
    assert normalize_col("Total Revenue ($)") == "total_revenue"
    assert normalize_col("col_2_x") == "col2_x"
    assert normalize_col("") == ""
    assert _coerce_numeric("(1,000)", "x") == -1000
    assert _coerce_numeric("$ 1,234.50 ", "x") == 1234.5
    assert _coerce_numeric("n/a", "x") is None
    assert _coerce_date("2026-10-01", "x") == datetime(2026, 10, 1)
    assert _coerce_date("-", "x") is None
    assert _mapping_report(["a", "b"], ["A", "C"]) == (["A"], ["C"], ["b"])

    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False,
                                     encoding="utf-8-sig", newline="") as fh:
        fh.write("Header One,Header 2\n1,x\n,\n")
        path = fh.name
    try:
        rows, names = parse_csv(path)
    finally:
        import os
        os.remove(path)
    assert names == ["header_one", "header2"], names
    assert rows == [{"header_one": "1", "header2": "x"}], rows
    print("csv_table self-check OK")


if __name__ == "__main__":
    _self_check()
