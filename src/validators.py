"""Data integrity validation for RCM Silver-layer tables.

All checks execute directly against the Silver tables in DuckDB — no
DataFrames need to be loaded.  This aligns with the medallion architecture:
validators assert the quality of the authoritative Silver layer.

Each check returns a list of issue dicts with keys:
    level   - "error" (data unusable) or "warning" (suspicious but recoverable)
    table   - which Silver table the issue was found in
    message - human-readable description
"""

import os

import duckdb

from src.database import DB_PATH, get_connection


def validate_all(db_path=None) -> list[dict]:
    """Run all validation checks against the Silver layer.

    Args:
        db_path: Optional path override for the DuckDB database.
                 Defaults to the configured DB_PATH.

    Returns:
        list of issue dicts (empty if all checks pass).
    """
    path = db_path or DB_PATH
    if not os.path.exists(path):
        return []  # Database not yet created — nothing to validate
    issues = []
    issues.extend(_check_negative_amounts(db_path))
    issues.extend(_check_orphaned_keys(db_path))
    issues.extend(_check_nulls(db_path))
    issues.extend(_check_date_ranges(db_path))
    issues.extend(_check_claim_status_values(db_path))
    issues.extend(_check_boolean_columns(db_path))
    return issues


# ── Individual checks ──────────────────────────────────────────────────────


def _check_negative_amounts(db_path=None) -> list[dict]:
    """Warn if monetary columns in Silver tables contain negative values."""
    checks = [
        ("silver_claims",          "total_charge_amount"),
        ("silver_payments",        "payment_amount"),
        ("silver_payments",        "allowed_amount"),
        ("silver_denials",         "denied_amount"),
        ("silver_denials",         "recovered_amount"),
        ("silver_adjustments",     "adjustment_amount"),
        ("silver_operating_costs", "total_rcm_cost"),
    ]
    issues = []
    conn = get_connection(db_path, read_only=True)
    try:
        for table, col in checks:
            try:
                n = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {col} < 0"
                ).fetchone()[0]
                if n > 0:
                    issues.append({
                        "level":   "warning",
                        "table":   table,
                        "message": f"{n} negative value(s) found in '{col}'.",
                    })
            except duckdb.Error:
                pass  # table or column doesn't exist — skip
    finally:
        conn.close()
    return issues


def _check_orphaned_keys(db_path=None) -> list[dict]:
    """Warn if foreign-key columns in Silver tables reference missing parent rows."""
    # (child_table, child_col, parent_table, parent_col)
    checks = [
        ("silver_payments",    "claim_id",    "silver_claims",    "claim_id"),
        ("silver_denials",     "claim_id",    "silver_claims",    "claim_id"),
        ("silver_adjustments", "claim_id",    "silver_claims",    "claim_id"),
        ("silver_claims",      "encounter_id","silver_encounters","encounter_id"),
        ("silver_claims",      "payer_id",    "silver_payers",    "payer_id"),
        ("silver_encounters",  "patient_id",  "silver_patients",  "patient_id"),
        ("silver_encounters",  "provider_id", "silver_providers", "provider_id"),
    ]
    issues = []
    conn = get_connection(db_path, read_only=True)
    try:
        for child_tbl, child_col, parent_tbl, parent_col in checks:
            try:
                n = conn.execute(f"""
                    SELECT COUNT(*) FROM {child_tbl} c
                    WHERE c.{child_col} IS NOT NULL
                      AND c.{child_col} NOT IN (
                          SELECT {parent_col} FROM {parent_tbl}
                      )
                """).fetchone()[0]
                if n > 0:
                    issues.append({
                        "level":   "warning",
                        "table":   child_tbl,
                        "message": (
                            f"{n} row(s) in '{child_tbl}.{child_col}' reference "
                            f"IDs not found in '{parent_tbl}.{parent_col}'."
                        ),
                    })
            except duckdb.Error:
                pass  # table doesn't exist — skip
    finally:
        conn.close()
    return issues


def _check_nulls(db_path=None) -> list[dict]:
    """Error if required key columns contain NULL values."""
    required_non_null = {
        "silver_claims":      ["claim_id", "patient_id", "payer_id",
                               "date_of_service", "total_charge_amount", "claim_status"],
        "silver_payments":    ["payment_id", "claim_id", "payment_amount"],
        "silver_denials":     ["denial_id",  "claim_id", "denied_amount"],
        "silver_adjustments": ["adjustment_id", "claim_id", "adjustment_amount"],
        "silver_encounters":  ["encounter_id", "patient_id", "provider_id", "date_of_service"],
        "silver_payers":      ["payer_id", "payer_name"],
    }
    issues = []
    conn = get_connection(db_path, read_only=True)
    try:
        for table, cols in required_non_null.items():
            for col in cols:
                try:
                    n = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
                    ).fetchone()[0]
                    if n > 0:
                        issues.append({
                            "level":   "error",
                            "table":   table,
                            "message": f"{n} null value(s) in required column '{table}.{col}'.",
                        })
                except duckdb.Error:
                    pass  # table or column doesn't exist — skip
    finally:
        conn.close()
    return issues


def _check_date_ranges(db_path=None) -> list[dict]:
    """Warn if date columns fall outside the plausible 2020–2030 range."""
    min_date = "2020-01-01"
    max_date = "2030-12-31"
    date_cols = {
        "silver_claims":    ["date_of_service", "submission_date"],
        "silver_payments":  ["payment_date"],
        "silver_denials":   ["denial_date"],
        "silver_encounters":["date_of_service"],
        "silver_charges":   ["service_date", "post_date"],
    }
    issues = []
    conn = get_connection(db_path, read_only=True)
    try:
        for table, cols in date_cols.items():
            for col in cols:
                try:
                    n = conn.execute(f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE {col} IS NOT NULL
                          AND ({col} < ? OR {col} > ?)
                    """, (min_date, max_date)).fetchone()[0]
                    if n > 0:
                        issues.append({
                            "level":   "warning",
                            "table":   table,
                            "message": (
                                f"{n} value(s) in '{table}.{col}' fall outside "
                                f"the expected range ({min_date} – {max_date})."
                            ),
                        })
                except duckdb.Error:
                    pass  # table or column doesn't exist — skip
    finally:
        conn.close()
    return issues


def _check_claim_status_values(db_path=None) -> list[dict]:
    """Warn if claim_status contains values outside the expected set."""
    valid = ("Paid", "Denied", "Appealed", "Pending", "Partially Paid")
    placeholders = ",".join("?" * len(valid))
    issues = []
    conn = get_connection(db_path, read_only=True)
    try:
        try:
            rows = conn.execute(f"""
                SELECT claim_status, COUNT(*) AS n
                FROM silver_claims
                WHERE claim_status NOT IN ({placeholders})
                GROUP BY claim_status
            """, valid).fetchall()
            if rows:
                bad_vals = [r[0] for r in rows]
                total = sum(r[1] for r in rows)
                issues.append({
                    "level":   "warning",
                    "table":   "silver_claims",
                    "message": (
                        f"{total} claim(s) have unexpected status value(s): {bad_vals}. "
                        f"Expected one of: {sorted(valid)}."
                    ),
                })
        except duckdb.Error:
            pass  # table doesn't exist — skip
    finally:
        conn.close()
    return issues


def _check_boolean_columns(db_path=None) -> list[dict]:
    """Warn if boolean columns contain values other than 0 or 1."""
    bool_checks = {
        "silver_claims":   ["is_clean_claim"],
        "silver_payments": ["is_accurate_payment"],
    }
    issues = []
    conn = get_connection(db_path, read_only=True)
    try:
        for table, cols in bool_checks.items():
            for col in cols:
                try:
                    n = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
                    ).fetchone()[0]
                    if n > 0:
                        issues.append({
                            "level":   "warning",
                            "table":   table,
                            "message": (
                                f"{n} null value(s) in boolean column '{table}.{col}' "
                                "(expected 0 or 1 only)."
                            ),
                        })
                except duckdb.Error:
                    pass  # table or column doesn't exist — skip
    finally:
        conn.close()
    return issues
