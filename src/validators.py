"""Data integrity validation for RCM datasets.

Each check appends a dict with keys:
    level   - "error" (data unusable) or "warning" (suspicious but recoverable)
    table   - which dataset the issue was found in
    message - human-readable description
"""

import pandas as pd


def validate_all(data: dict) -> list[dict]:
    """Run all validation checks against the loaded data dict.

    Returns a list of issue dicts (may be empty if data is clean).
    """
    issues = []
    issues.extend(_check_negative_amounts(data))
    issues.extend(_check_orphaned_keys(data))
    issues.extend(_check_nulls(data))
    issues.extend(_check_date_ranges(data))
    issues.extend(_check_claim_status_values(data))
    issues.extend(_check_boolean_columns(data))
    return issues


# ── Individual checks ─────────────────────────────────────────────────────────

def _check_negative_amounts(data: dict) -> list[dict]:
    """Warn if monetary columns contain negative values."""
    issues = []
    checks = [
        ("claims",          "total_charge_amount"),
        ("payments",        "payment_amount"),
        ("payments",        "allowed_amount"),
        ("denials",         "denied_amount"),
        ("denials",         "recovered_amount"),
        ("adjustments",     "adjustment_amount"),
        ("operating_costs", "total_rcm_cost"),
    ]
    for table, col in checks:
        df = data.get(table)
        if df is None or col not in df.columns:
            continue
        n = (df[col] < 0).sum()
        if n > 0:
            issues.append({
                "level": "warning",
                "table": table,
                "message": f"{n} negative value(s) found in '{col}'.",
            })
    return issues


def _check_orphaned_keys(data: dict) -> list[dict]:
    """Warn if foreign keys reference IDs that don't exist in the parent table."""
    issues = []
    checks = [
        # (child_table, child_col, parent_table, parent_col)
        ("payments",    "claim_id",   "claims",   "claim_id"),
        ("denials",     "claim_id",   "claims",   "claim_id"),
        ("adjustments", "claim_id",   "claims",   "claim_id"),
        ("claims",      "encounter_id", "encounters", "encounter_id"),
        ("claims",      "payer_id",   "payers",   "payer_id"),
        ("encounters",  "patient_id", "patients", "patient_id"),
        ("encounters",  "provider_id","providers","provider_id"),
    ]
    for child_table, child_col, parent_table, parent_col in checks:
        child = data.get(child_table)
        parent = data.get(parent_table)
        if child is None or parent is None:
            continue
        if child_col not in child.columns or parent_col not in parent.columns:
            continue
        orphans = ~child[child_col].isin(parent[parent_col])
        n = orphans.sum()
        if n > 0:
            issues.append({
                "level": "warning",
                "table": child_table,
                "message": (
                    f"{n} row(s) in '{child_table}.{child_col}' reference "
                    f"IDs not found in '{parent_table}.{parent_col}'."
                ),
            })
    return issues


def _check_nulls(data: dict) -> list[dict]:
    """Error if required key columns contain null values."""
    issues = []
    required_non_null = {
        "claims":      ["claim_id", "patient_id", "payer_id", "date_of_service",
                        "total_charge_amount", "claim_status"],
        "payments":    ["payment_id", "claim_id", "payment_amount"],
        "denials":     ["denial_id", "claim_id", "denied_amount"],
        "adjustments": ["adjustment_id", "claim_id", "adjustment_amount"],
        "encounters":  ["encounter_id", "patient_id", "provider_id", "date_of_service"],
        "payers":      ["payer_id", "payer_name"],
    }
    for table, cols in required_non_null.items():
        df = data.get(table)
        if df is None:
            continue
        for col in cols:
            if col not in df.columns:
                continue
            n = df[col].isna().sum()
            if n > 0:
                issues.append({
                    "level": "error",
                    "table": table,
                    "message": f"{n} null value(s) in required column '{table}.{col}'.",
                })
    return issues


def _check_date_ranges(data: dict) -> list[dict]:
    """Warn if dates fall outside the plausible 2020–2030 range."""
    issues = []
    min_date = pd.Timestamp("2020-01-01")
    max_date = pd.Timestamp("2030-12-31")

    date_cols = {
        "claims":    ["date_of_service", "submission_date"],
        "payments":  ["payment_date"],
        "denials":   ["denial_date"],
        "encounters":["date_of_service"],
        "charges":   ["service_date", "post_date"],
    }
    for table, cols in date_cols.items():
        df = data.get(table)
        if df is None:
            continue
        for col in cols:
            if col not in df.columns:
                continue
            series = pd.to_datetime(df[col], errors="coerce")
            out_of_range = series.notna() & ((series < min_date) | (series > max_date))
            n = out_of_range.sum()
            if n > 0:
                issues.append({
                    "level": "warning",
                    "table": table,
                    "message": (
                        f"{n} value(s) in '{table}.{col}' fall outside "
                        f"the expected range ({min_date.date()} – {max_date.date()})."
                    ),
                })
    return issues


def _check_claim_status_values(data: dict) -> list[dict]:
    """Warn if claim_status contains unexpected values."""
    issues = []
    claims = data.get("claims")
    if claims is None or "claim_status" not in claims.columns:
        return issues
    valid_statuses = {"Paid", "Denied", "Appealed", "Pending"}
    unknown = ~claims["claim_status"].isin(valid_statuses)
    n = unknown.sum()
    if n > 0:
        bad_vals = claims.loc[unknown, "claim_status"].unique().tolist()
        issues.append({
            "level": "warning",
            "table": "claims",
            "message": (
                f"{n} claim(s) have unexpected status value(s): {bad_vals}. "
                f"Expected one of: {sorted(valid_statuses)}."
            ),
        })
    return issues


def _check_boolean_columns(data: dict) -> list[dict]:
    """Warn if boolean columns contain nulls (failed to parse)."""
    issues = []
    bool_checks = {
        "claims":   ["is_clean_claim"],
        "payments": ["is_accurate_payment"],
    }
    for table, cols in bool_checks.items():
        df = data.get(table)
        if df is None:
            continue
        for col in cols:
            if col not in df.columns:
                continue
            n = df[col].isna().sum()
            if n > 0:
                issues.append({
                    "level": "warning",
                    "table": table,
                    "message": (
                        f"{n} null value(s) in boolean column '{table}.{col}' "
                        "(expected True/False only)."
                    ),
                })
    return issues
