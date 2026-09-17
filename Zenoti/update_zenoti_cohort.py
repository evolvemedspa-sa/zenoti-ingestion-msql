"""
update_zenoti_cohort.py

Rebuilds [dbo].[BRONZE_ZENOTI_COHORT] -- the union of the three historical
customer-spend sources -- from the three bronze tables that feed it:

    src     source table                                 window
    SQ      dbo.BRONZE_SQUARE_TRANSACTIONS_2020_2022     2020-05-11 .. 2021-09-13
    BLVD    dbo.BRONZE_BLVD_CLIENT_SPEND_TXN             2021-09-14 .. 2022-10-05
    ZEN     dbo.BRONZE_ZENOTI_CASH_COLLECTIONS           2022-10-06 .. today

The two historical legs cover CLOSED windows; only the ZEN leg is open-ended
(<= GETDATE()), so it grows every day. The three date ranges are adjacent and
non-overlapping, which is what makes a single chronological union well-defined.

TARGET SHAPE (src, full_name, sale_date, amount, name_key). One output row per
qualifying source row -- there is deliberately no de-duplication: a customer with
three purchases on one day is three rows, and the dashboard's per-customer rollup
sums them. The table must ALREADY EXIST (created out-of-band; this project never
runs DDL from Python).

An optional [date_inserted] audit column records when the table was last
refreshed; the table's DEFAULT constraint fills it, so the INSERTs below never
mention it. See New folder/alter_zenoti_cohort_add_date_inserted.sql.

WHY THIS IS NOT A CSV LOADER
Unlike every other importer in this folder, the source here is not a flat file --
it is three tables in the SAME database. So nothing is round-tripped through
pandas; the three INSERT..SELECT statements run entirely server-side. That is one
to two orders of magnitude faster, and -- more importantly -- it keeps the SQL
below BYTE-FOR-BYTE IDENTICAL to the statements that were previously hand-run in
SSMS. The CROSS APPLY tender normalization and the whole-token LIKE matching in
the ZEN leg, and the test-account exclusions in all three, are subtle enough that
re-implementing them in Python would silently drift.

Load mode: FULL REPLACE. Each run deletes the three src values and re-inserts all
three legs, in ONE transaction (rollback on error). Re-running is therefore
idempotent -- which is precisely the bug the manual process has today, where a
second run of those INSERTs silently doubles every row.

Usage:
    python update_zenoti_cohort.py                # rebuild the table
    python update_zenoti_cohort.py --check-db     # read-only: report + preview, no writes
    python update_zenoti_cohort.py --dry-run      # print the SQL; never connects
    python update_zenoti_cohort.py --allow-empty  # permit a leg to return 0 rows
"""

import argparse
import os
import sys

import pyodbc
from dotenv import load_dotenv

from db_helper import get_connection

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

TARGET_TABLE = "dbo.BRONZE_ZENOTI_COHORT"
TARGET_COLUMNS = ("src", "full_name", "sale_date", "amount", "name_key")

# Audit column, stamped by the table's own DEFAULT constraint (see
# New folder/alter_zenoti_cohort_add_date_inserted.sql). It is deliberately NOT
# in TARGET_COLUMNS: the INSERTs below omit it, so the DEFAULT fires and the
# three leg statements stay byte-for-byte identical to the hand-run originals.
# Its absence is a warning, never an error -- the load works without it, it just
# does not record when it last ran.
AUDIT_COLUMN = "date_inserted"

# =============================================================================
# The three legs. Each is the SELECT body of the original hand-run statement --
# verbatim, including comments -- so it stays reviewable and diffable. The
# INSERT wrapper and the column list are added programmatically below, which is
# also what lets the same text be reused for the read-only --check-db preview.
# =============================================================================

SQ_SELECT = """
SELECT
    'SQ' AS src,
    customer_name AS full_name,
    CAST(transaction_date AS DATE) AS sale_date,
    net_sales AS amount,
    UPPER(LTRIM(RTRIM(customer_name))) AS name_key
FROM dbo.BRONZE_SQUARE_TRANSACTIONS_2020_2022
WHERE customer_name IS NOT NULL AND LTRIM(RTRIM(customer_name)) <> ''
  AND net_sales IS NOT NULL
  AND net_sales <> 0
  AND CAST(transaction_date AS DATE) >= '2020-05-11'
  AND CAST(transaction_date AS DATE) <= '2021-09-13'
  -- Exclude internal test accounts. Whitespace collapsed for the comparison
  -- only, so name_key itself is untouched; catches "TEST  TEST".
  AND REPLACE(REPLACE(REPLACE(UPPER(LTRIM(RTRIM(customer_name))), CHAR(9), ' '), '  ', ' '), '  ', ' ')
      NOT IN ('TEST TEST', 'TEST ONLY')
"""

BLVD_SELECT = """
SELECT
    'BLVD' AS src,
    client_name AS full_name,
    CAST(sale_date AS DATE) AS sale_date,
    net_sales_plus_redeemed_vouchers AS amount,
    UPPER(LTRIM(RTRIM(client_name))) AS name_key
FROM dbo.BRONZE_BLVD_CLIENT_SPEND_TXN
WHERE client_name IS NOT NULL AND LTRIM(RTRIM(client_name)) <> ''
  AND net_sales_plus_redeemed_vouchers IS NOT NULL
  AND net_sales_plus_redeemed_vouchers <> 0
  AND CAST(sale_date AS DATE) >= '2021-09-14'
  AND CAST(sale_date AS DATE) <= '2022-10-05'
  -- Same test-account exclusion as the other two legs.
  AND REPLACE(REPLACE(REPLACE(UPPER(LTRIM(RTRIM(client_name))), CHAR(9), ' '), '  ', ' '), '  ', ' ')
      NOT IN ('TEST TEST', 'TEST ONLY')
"""

# Tender filter is a verbatim port of the dashboard's _CASH_PAY_FILTER
# (evolve-api/routers/mtd.py). payment_type holds a comma-separated list of
# ALL tenders on an invoice, so we normalise to ",tok1,tok2," and match WHOLE
# delimited tokens. A naive '%card%' would wrongly match "Gift Card" /
# "Prepaid Card" and '%cash%' would match "Cashback", leaking redemption-only
# rows (no money collected) into the total.
# NOTE: no item_type filter -- the dashboard applies none, and pre-Dec-2024
# item_type is unreliable (79% of 2024 "Product" is neurotoxin, not retail).
ZEN_SELECT = """
SELECT
    'ZEN' AS src,
    z.guest_name AS full_name,
    CAST(z.payment_date AS DATE) AS sale_date,
    z.sales_collected_exc_tax AS amount,
    UPPER(LTRIM(RTRIM(z.guest_name))) AS name_key
FROM dbo.BRONZE_ZENOTI_CASH_COLLECTIONS AS z
CROSS APPLY (VALUES (
    ',' + REPLACE(LOWER(LTRIM(RTRIM(z.payment_type))), ', ', ',') + ','
)) AS pt(norm)
WHERE z.guest_name IS NOT NULL AND LTRIM(RTRIM(z.guest_name)) <> ''
  AND z.sales_collected_exc_tax IS NOT NULL
  AND z.sales_collected_exc_tax <> 0
  AND CAST(z.payment_date AS DATE) >= '2022-10-06'
  AND CAST(z.payment_date AS DATE) <= CAST(GETDATE() AS DATE)
  AND (
        pt.norm LIKE '%,card,%'
     OR pt.norm LIKE '%,cash,%'
     OR pt.norm LIKE '%,check,%'
     OR pt.norm LIKE '%,custom - %'
      )
  -- Same test-account exclusion as the other two legs.
  AND REPLACE(REPLACE(REPLACE(UPPER(LTRIM(RTRIM(z.guest_name))), CHAR(9), ' '), '  ', ' '), '  ', ' ')
      NOT IN ('TEST TEST', 'TEST ONLY')
"""

# (src, source table, SELECT body) -- order is chronological.
LEGS = (
    ("SQ", "dbo.BRONZE_SQUARE_TRANSACTIONS_2020_2022", SQ_SELECT),
    ("BLVD", "dbo.BRONZE_BLVD_CLIENT_SPEND_TXN", BLVD_SELECT),
    ("ZEN", "dbo.BRONZE_ZENOTI_CASH_COLLECTIONS", ZEN_SELECT),
)


def _split_table(qualified):
    """'dbo.NAME' / '[dbo].[NAME]' / 'NAME' -> ('dbo', 'NAME')."""
    schema, name = "dbo", qualified
    if "." in qualified:
        left, right = qualified.split(".", 1)
        schema = left.strip(" []")
        name = right.strip(" []")
    return schema, name.strip(" []")


def _rule(char="-", width=70):
    print(char * width, flush=True)


def _leg_insert_sql(body):
    """Wrap a leg's SELECT body in the INSERT that targets the cohort table."""
    cols = ",".join(f"[{c}]" for c in TARGET_COLUMNS)
    return f"INSERT INTO {TARGET_TABLE} ({cols})\n{body}"


def _leg_preview_sql(body):
    """Wrap a leg's SELECT body so it can be COUNTed without writing anything.

    The body's aliases (src, full_name, sale_date, amount, name_key) are unique,
    so it is a valid derived table and its sale_date is addressable from outside.
    """
    return (
        "SELECT COUNT(*) AS n, MIN(sale_date) AS min_date, MAX(sale_date) AS max_date\n"
        f"FROM (\n{body}\n) AS leg"
    )


def _delete_sql():
    """Scoped DELETE: clears exactly the three src values this script owns.

    A bare 'DELETE FROM' would be equivalent today, but this way a fourth source
    added to the table later is not silently wiped by a cohort rebuild.
    """
    srcs = ",".join(f"'{src}'" for src, _t, _b in LEGS)
    return f"DELETE FROM {TARGET_TABLE} WHERE src IN ({srcs})"


def _table_exists(cursor, qualified):
    """True if the (schema-qualified) table exists as a user table."""
    schema, name = _split_table(qualified)
    cursor.execute("SELECT OBJECT_ID(?, 'U')", (f"{schema}.{name}",))
    row = cursor.fetchone()
    return bool(row and row[0] is not None)


def _table_columns(cursor, schema, name):
    """Return (columns, identity_columns) for a table, in ordinal position."""
    cursor.execute(
        """
        SELECT c.COLUMN_NAME,
               COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)),
                              c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        WHERE c.TABLE_NAME = ? AND c.TABLE_SCHEMA = ?
        ORDER BY c.ORDINAL_POSITION
        """,
        (name, schema),
    )
    fetched = cursor.fetchall()
    return [r[0] for r in fetched], {r[0] for r in fetched if r[1] == 1}


def _print_plan():
    """Print the exact SQL a real run would execute. Used by --dry-run."""
    print("SQL that a real run would execute, in order, in ONE transaction:")
    print()
    for i, stmt in enumerate(_all_statements(), 1):
        _rule()
        print(f"-- statement {i}")
        print(stmt.strip())
    _rule()
    print(f"NOTE: [{AUDIT_COLUMN}] is absent from every INSERT above by design -- the")
    print(f"      table's own DEFAULT constraint stamps it, which is what keeps the")
    print(f"      three leg statements identical to the originals. If the column does")
    print(f"      not exist yet, see New folder/alter_zenoti_cohort_add_date_inserted.sql")
    print()
    print("No connection was made; nothing was read or written.")


def _all_statements():
    """The ordered statement list of a real run (excluding the pre-flight counts)."""
    stmts = [_delete_sql()]
    stmts.extend(_leg_insert_sql(body) for _src, _t, body in LEGS)
    stmts.append(f"UPDATE STATISTICS {TARGET_TABLE}")
    return stmts


def _check_db(conn):
    """Read-only inspection. Every statement on this path is a SELECT.

    Reports current table state, what each leg would produce RIGHT NOW, and the
    index inventory -- so the effect of a rebuild is known before committing to
    one. Returns nothing; the caller decides whether to proceed.
    """
    cursor = conn.cursor()

    print(f"Target: {TARGET_TABLE}")
    _rule()
    if not _table_exists(cursor, TARGET_TABLE):
        print(f"  MISSING -- {TARGET_TABLE} does not exist. Create it out-of-band first.")
        return
    schema, name = _split_table(TARGET_TABLE)
    columns, identity = _table_columns(cursor, schema, name)
    print(f"  exists. {len(columns)} column(s): {', '.join(columns)}")
    if identity:
        print(f"  identity column(s): {', '.join(sorted(identity))}")

    missing_cols = [c for c in TARGET_COLUMNS if c not in columns]
    if missing_cols:
        print(f"  !! MISSING REQUIRED COLUMN(S): {', '.join(missing_cols)} -- the "
              f"INSERTs below would fail.")
    else:
        print(f"  required columns present: {', '.join(TARGET_COLUMNS)}")

    has_audit = AUDIT_COLUMN in columns
    if has_audit:
        print(f"  audit column present: {AUDIT_COLUMN} (rows are stamped by the "
              f"table DEFAULT on insert)")
    else:
        print(f"  NOTE: no [{AUDIT_COLUMN}] column -- this table does not record when "
              f"it was last refreshed.")
        print(f"        Add it with New folder/alter_zenoti_cohort_add_date_inserted.sql")

    # --- source tables, so a typo'd/renamed upstream is caught up front -------
    print()
    print("Source tables")
    _rule()
    for src, table, _body in LEGS:
        ok = _table_exists(cursor, table)
        print(f"  [{src:4}] {table}  {'OK' if ok else '!! MISSING'}")

    # --- what is in the table right now --------------------------------------
    print()
    print(f"Current contents of {TARGET_TABLE}")
    _rule()
    audit_sel = f", MAX({AUDIT_COLUMN}) AS last_refreshed" if has_audit else ""
    cursor.execute(
        f"SELECT src, COUNT(*) AS n, MIN(sale_date) AS min_d, MAX(sale_date) AS max_d"
        f"{audit_sel} "
        f"FROM {TARGET_TABLE} GROUP BY src ORDER BY src"
    )
    rows = cursor.fetchall()
    if not rows:
        print("  (table is empty)")
    total = 0
    refreshed = []
    for row in rows:
        src, n, min_d, max_d = row[:4]
        total += n
        stamp = ""
        if has_audit:
            stamp = f"   loaded {row[4]}"
            if row[4] is not None:
                refreshed.append(row[4])
        print(f"  [{src:4}] {n:>10,} rows   {min_d} .. {max_d}{stamp}")
    if rows:
        print(f"  {'TOTAL':<6} {total:>10,} rows")
        if refreshed:
            print(f"  last refresh (MAX {AUDIT_COLUMN}): {max(refreshed)}")

    # --- what each leg would produce right now (no writes) -------------------
    print()
    print("What each leg SELECT would produce right now (read-only preview)")
    _rule()
    spans = []
    for src, _table, body in LEGS:
        try:
            cursor.execute(_leg_preview_sql(body))
            n, min_d, max_d = cursor.fetchone()
        except pyodbc.Error as e:
            print(f"  [{src:4}] QUERY FAILED: {e}")
            continue
        spans.append((src, n, min_d, max_d))
        print(f"  [{src:4}] {n:>10,} rows   {min_d} .. {max_d}")

    # The union is only well-defined if the windows stay adjacent and ordered.
    # Flag an overlap rather than assuming the hardcoded dates still hold.
    print()
    print("Leg window check")
    _rule()
    ordered = [(s, n, lo, hi) for s, n, lo, hi in spans if lo is not None]
    if len(ordered) < 2:
        print("  (not enough populated legs to compare)")
    else:
        clean = True
        for (s1, _n1, _lo1, hi1), (s2, _n2, lo2, _hi2) in zip(ordered, ordered[1:]):
            if hi1 >= lo2:
                clean = False
                print(f"  !! OVERLAP: {s1} ends {hi1} but {s2} starts {lo2} -- "
                      f"the same period would be counted twice.")
        if clean:
            print("  OK -- the three windows are adjacent and non-overlapping.")

    # --- name_key hygiene ----------------------------------------------------
    # The dashboard rolls up PER CUSTOMER by name_key, and name_key is
    # UPPER(LTRIM(RTRIM(...))) -- it trims the ends but NOT internal runs. So
    # "JOHN  SMITH" and "JOHN SMITH" are two different customers to the rollup.
    # Reported, never silently changed: collapsing them would move the numbers
    # the dashboard shows.
    print()
    print("name_key hygiene (affects the dashboard's per-customer rollup)")
    _rule()
    collapsed = ("REPLACE(REPLACE(REPLACE(name_key, CHAR(9), ' '), '  ', ' '), '  ', ' ')")
    cursor.execute(
        f"SELECT COUNT(DISTINCT name_key) AS distinct_keys, "
        f"       COUNT(DISTINCT {collapsed}) AS distinct_collapsed "
        f"FROM {TARGET_TABLE}"
    )
    distinct_keys, distinct_collapsed = cursor.fetchone()
    print(f"  distinct name_key        : {distinct_keys:,}")
    print(f"  distinct after collapsing: {distinct_collapsed:,}")
    if distinct_keys and distinct_collapsed and distinct_keys > distinct_collapsed:
        print(f"  NOTE: {distinct_keys - distinct_collapsed:,} key(s) differ only by "
              f"internal whitespace and roll up as separate customers.")
    else:
        print("  OK -- no keys differ only by internal whitespace.")

    # --- indexes -------------------------------------------------------------
    print()
    print("Index inventory")
    _rule()
    cursor.execute(
        """
        SELECT i.name AS index_name, i.type_desc, i.is_primary_key, i.is_unique,
               i.is_disabled, ic.is_included_column, ic.key_ordinal,
               COL_NAME(ic.object_id, ic.column_id) AS col_name
        FROM sys.indexes i
        LEFT JOIN sys.index_columns ic
               ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        WHERE i.object_id = OBJECT_ID(?)
          AND i.type IN (1, 2)
        ORDER BY i.index_id, ic.key_ordinal, ic.index_column_id
        """,
        (TARGET_TABLE,),
    )
    rows = cursor.fetchall()
    if not rows:
        print("  none -- table is a HEAP with no clustered or nonclustered index.")
        print("  The dashboard's per-customer rollup will scan. See "
              "New folder/create_zenoti_cohort_indexes.sql")
    else:
        groups = {}
        for r in rows:
            groups.setdefault(r[0], []).append(r)
        for idx_name, cols in groups.items():
            keys = [c[7] for c in cols if c[5] == 0]
            inc = [c[7] for c in cols if c[5] == 1]
            print(f"  {idx_name}  type={cols[0][1]}  PK={cols[0][2]}  "
                  f"unique={cols[0][3]}  disabled={cols[0][4]}")
            print(f"      keys=({', '.join(keys)})" +
                  (f"  include=({', '.join(inc)})" if inc else ""))

    cursor.close()
    print()
    print("Read-only check complete. Nothing was modified.")


def _run(conn, allow_empty):
    """Rebuild the cohort in one transaction. Rolls back on any failure."""
    cursor = conn.cursor()

    # Pre-flight: the target must exist before we delete anything from it.
    if not _table_exists(cursor, TARGET_TABLE):
        raise RuntimeError(
            f"{TARGET_TABLE} does not exist. Create it out-of-band first "
            f"(this project never runs DDL from Python)."
        )
    for src, table, _body in LEGS:
        if not _table_exists(cursor, table):
            raise RuntimeError(f"source table for [{src}] not found: {table}")

    # The audit column is optional -- warn rather than fail. Without it the load
    # still works, it just leaves no record of when it ran.
    schema, name = _split_table(TARGET_TABLE)
    columns, _identity = _table_columns(cursor, schema, name)
    has_audit = AUDIT_COLUMN in columns
    if not has_audit:
        print(f"NOTE: {TARGET_TABLE} has no [{AUDIT_COLUMN}] column, so this run will "
              f"not be recorded.", flush=True)
        print(f"      Add it with New folder/alter_zenoti_cohort_add_date_inserted.sql",
              flush=True)

    try:
        # Count every leg BEFORE the delete, inside the same transaction. A leg
        # that suddenly returns 0 rows means an upstream load broke or a window
        # moved -- better to abort with nothing changed than to publish an empty
        # cohort to a live dashboard.
        print("Counting each leg before touching the table...", flush=True)
        counts = {}
        for src, _table, body in LEGS:
            cursor.execute(_leg_preview_sql(body))
            n, min_d, max_d = cursor.fetchone()
            counts[src] = n
            print(f"  [{src:4}] {n:>10,} rows   {min_d} .. {max_d}", flush=True)

        empty = [src for src, n in counts.items() if n == 0]
        if empty and not allow_empty:
            raise RuntimeError(
                f"leg(s) {', '.join(empty)} returned 0 rows. Aborting with the table "
                f"untouched -- re-run with --allow-empty if this is genuinely expected."
            )

        print(f"\nDeleting existing rows ({_delete_sql()})...", flush=True)
        cursor.execute(_delete_sql())
        print(f"  deleted {cursor.rowcount:,} row(s)", flush=True)

        for src, _table, body in LEGS:
            print(f"Inserting leg [{src}]...", flush=True)
            cursor.execute(_leg_insert_sql(body))
            print(f"  inserted {cursor.rowcount:,} row(s)", flush=True)
            if cursor.rowcount != counts[src]:
                raise RuntimeError(
                    f"leg [{src}] inserted {cursor.rowcount:,} rows but its pre-count "
                    f"said {counts[src]:,} -- the source changed mid-transaction."
                )

        # A full DELETE empties the table's statistics with it, which can push
        # the optimizer into a poor plan for the very rollup this table serves.
        print("Updating statistics...", flush=True)
        cursor.execute(f"UPDATE STATISTICS {TARGET_TABLE}")

        conn.commit()
        print("\nCommitted.", flush=True)

    except Exception:
        conn.rollback()
        print("\nRolled back -- the table is unchanged.", flush=True)
        raise

    # Post-load verification, read back from the committed table.
    print()
    print(f"Verified contents of {TARGET_TABLE}")
    _rule()
    audit_sel = f", MAX({AUDIT_COLUMN}) AS last_refreshed" if has_audit else ""
    cursor.execute(
        f"SELECT src, COUNT(*) AS n, MIN(sale_date) AS min_d, MAX(sale_date) AS max_d"
        f"{audit_sel} "
        f"FROM {TARGET_TABLE} GROUP BY src ORDER BY src"
    )
    total = 0
    refreshed = []
    rows = cursor.fetchall()
    for row in rows:
        src, n, min_d, max_d = row[:4]
        total += n
        print(f"  [{src:4}] {n:>10,} rows   {min_d} .. {max_d}")
        if has_audit and row[4] is not None:
            refreshed.append(row[4])
    print(f"  {'TOTAL':<6} {total:>10,} rows")
    if refreshed:
        print(f"  last refresh (MAX {AUDIT_COLUMN}): {max(refreshed)}")
    cursor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Rebuild dbo.BRONZE_ZENOTI_COHORT from its three source tables."
    )
    parser.add_argument("--check-db", action="store_true",
                        help="read-only: connect, report current state + a preview of "
                             "each leg, and make no changes")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the SQL that would run; never connects to the DB")
    parser.add_argument("--allow-empty", action="store_true",
                        help="permit a leg to return 0 rows instead of aborting")
    args = parser.parse_args()

    if args.dry_run:
        _print_plan()
        return 0

    conn = get_connection()
    try:
        if args.check_db:
            _check_db(conn)
            return 0
        _run(conn, allow_empty=args.allow_empty)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as err:
        print(f"\nFAILED: {err}", file=sys.stderr)
        sys.exit(1)
