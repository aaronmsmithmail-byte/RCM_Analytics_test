"""
Data Loader for Healthcare RCM Analytics Dashboard
===================================================

This module is the bridge between the Silver layer of the DuckDB Medallion
Architecture and the Streamlit dashboard.  It loads all 10 Silver tables into
pandas DataFrames, handling date parsing and type conversions so that the
metrics engine can work with clean, typed data.

Medallion Architecture Data Flow:
    CSV Files
        ↓ (raw ingestion)
    Bronze tables  (bronze_*, all TEXT, _loaded_at timestamp)
        ↓ (ETL — type casting, validation)
    Silver tables  (silver_*, typed + FK-constrained)  ← this module reads here
        ↓ (aggregation views)
    Gold views     (gold_*, pre-joined SQL aggregates)  ← load_gold_data() reads here
        ↓
    DataFrames  →  Metrics Engine  →  Dashboard

Why load Silver into DataFrames instead of querying Gold views for everything?
    - pandas groupby/pivot/merge operations power the interactive sidebar filters
      (date range, payer, department) which must re-slice the data on every
      Streamlit rerun.
    - Gold views are fixed aggregations; Silver DataFrames let the metrics engine
      slice in any dimension the user requests.
    - With Streamlit's @st.cache_data, the Silver DataFrames are cached in
      memory so subsequent page loads are instant.
    - For our data volume (~3,000 encounters), loading Silver into memory is
      fast and keeps the metrics layer simple.

Usage:
    from src.data_loader import load_all_data, load_gold_data
    data = load_all_data()      # Silver layer → dict of DataFrames
    gold = load_gold_data()     # Gold layer  → dict of pre-aggregated DataFrames
"""

import pandas as pd

from src.database import DB_PATH, has_medallion_schema, initialize_database, query_to_dataframe  # noqa: F401


def _parse_dates(df, date_columns):
    """
    Convert string date columns to pandas Timestamp objects.

    DuckDB stores dates as TEXT strings (e.g., "2024-06-15"). We need to
    convert them to pandas datetime objects so that:
    - Date range filtering works with comparison operators (>=, <=).
    - We can extract year/month for trend analysis (dt.to_period("M")).
    - Plotly can render proper time-series axes.

    Args:
        df:            The DataFrame to modify (modified in-place for efficiency).
        date_columns:  List of column names that contain date strings.

    Returns:
        The same DataFrame with date columns converted.

    Notes:
        - errors="coerce" converts unparseable values to NaT (Not a Time)
          instead of raising an error. This handles empty/null date fields
          gracefully (e.g., appeal_date is empty for non-appealed denials).
    """
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _parse_booleans(df, bool_columns):
    """
    Convert integer boolean columns from DuckDB (0/1) to Python booleans.

    The Silver ETL casts 'True'/'False' strings to INTEGER (1/0).
    The metrics engine expects Python booleans so that .sum() counts
    True values correctly.

    Args:
        df:            The DataFrame to modify.
        bool_columns:  List of column names that contain 0/1 boolean values.

    Returns:
        The same DataFrame with boolean columns converted.
    """
    for col in bool_columns:
        if col in df.columns:
            df[col] = df[col].astype(bool)
    return df


# Required columns for each table — used for validation after load.
# Keys match the un-prefixed logical table names (what the rest of the app uses).
REQUIRED_COLUMNS = {
    "payers": ["payer_id", "payer_name", "payer_type"],
    "patients": ["patient_id", "primary_payer_id"],
    "providers": ["provider_id", "department"],
    "encounters": ["encounter_id", "patient_id", "provider_id", "date_of_service", "department", "encounter_type"],
    "charges": ["charge_id", "encounter_id", "charge_amount", "service_date", "post_date"],
    "claims": ["claim_id", "encounter_id", "patient_id", "payer_id", "date_of_service",
               "submission_date", "total_charge_amount", "claim_status", "is_clean_claim"],
    "payments": ["payment_id", "claim_id", "payer_id", "payment_amount", "is_accurate_payment"],
    "denials": ["denial_id", "claim_id", "denial_reason_code", "denial_reason_description",
                "denied_amount", "appeal_status", "recovered_amount"],
    "adjustments": ["adjustment_id", "claim_id", "adjustment_type_code", "adjustment_amount"],
    "operating_costs": ["period", "total_rcm_cost"],
}


def _validate_columns(df, key, path):
    """Raise ValueError if any required columns are missing from the DataFrame."""
    required = REQUIRED_COLUMNS.get(key, [])
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Data source '{path}' is missing required columns: {missing}"
        )


def load_all_data():
    """
    Load all 10 Silver-layer tables from DuckDB into a dict of DataFrames.

    This is the primary function called by the Streamlit dashboard at startup.
    It reads from the silver_* tables (cleaned, typed, FK-validated data),
    parses dates and booleans, and returns everything in a single dict.

    Returns:
        dict: Keys are logical table names (without the silver_ prefix).
            {
                "payers":          DataFrame (~10 rows),
                "patients":        DataFrame (~500 rows),
                "providers":       DataFrame (~25 rows),
                "encounters":      DataFrame (~3,000 rows),
                "charges":         DataFrame (~5,900 rows),
                "claims":          DataFrame (~2,800 rows),
                "payments":        DataFrame (~2,700 rows),
                "denials":         DataFrame (~400 rows),
                "adjustments":     DataFrame (~600 rows),
                "operating_costs": DataFrame (~24 rows),
            }

    Auto-initialization:
        If the database does not exist or lacks the Silver schema, this
        function automatically runs initialize_database() to build the full
        medallion architecture from the CSV source files.
    """
    # ------------------------------------------------------------------
    # Auto-initialize if the Silver layer is not yet present.
    # This covers both "first run" (no DB file) and "schema migration"
    # (old DB exists but uses the pre-medallion un-prefixed table names).
    # ------------------------------------------------------------------
    if not has_medallion_schema():
        print("Silver layer not found. Running medallion architecture init...")
        initialize_database()
    else:
        # Always refresh meta tables so source_system, KG nodes/edges, KPI
        # definitions, and semantic mappings stay in sync with the codebase.
        from src.database import get_connection, persist_metadata
        _conn = get_connection()
        persist_metadata(_conn)
        _conn.commit()
        _conn.close()

    # ------------------------------------------------------------------
    # Table configuration: logical name → SQL query + parse hints.
    # All queries target silver_* tables in the database.
    # ------------------------------------------------------------------
    table_config = {
        "payers": {
            "query": "SELECT * FROM silver_payers",
            "date_cols": [],
            "bool_cols": [],
        },
        "patients": {
            "query": "SELECT * FROM silver_patients",
            "date_cols": ["date_of_birth"],
            "bool_cols": [],
        },
        "providers": {
            "query": "SELECT * FROM silver_providers",
            "date_cols": [],
            "bool_cols": [],
        },
        "encounters": {
            "query": "SELECT * FROM silver_encounters",
            "date_cols": ["date_of_service", "discharge_date"],
            "bool_cols": [],
        },
        "charges": {
            "query": "SELECT * FROM silver_charges",
            "date_cols": ["service_date", "post_date"],
            "bool_cols": [],
        },
        "claims": {
            "query": "SELECT * FROM silver_claims",
            "date_cols": ["date_of_service", "submission_date"],
            "bool_cols": ["is_clean_claim"],
        },
        "payments": {
            "query": "SELECT * FROM silver_payments",
            "date_cols": ["payment_date"],
            "bool_cols": ["is_accurate_payment"],
        },
        "denials": {
            "query": "SELECT * FROM silver_denials",
            "date_cols": ["denial_date", "appeal_date"],
            "bool_cols": [],
        },
        "adjustments": {
            "query": "SELECT * FROM silver_adjustments",
            "date_cols": ["adjustment_date"],
            "bool_cols": [],
        },
        "operating_costs": {
            "query": "SELECT * FROM silver_operating_costs",
            "date_cols": [],  # 'period' handled specially below
            "bool_cols": [],
        },
    }

    # ------------------------------------------------------------------
    # Load each Silver table into a DataFrame
    # ------------------------------------------------------------------
    data = {}
    for table_name, config in table_config.items():
        df = query_to_dataframe(config["query"])
        _parse_dates(df, config["date_cols"])
        _parse_booleans(df, config["bool_cols"])

        # 'period' in operating_costs is "YYYY-MM"; parse to datetime
        if table_name == "operating_costs" and "period" in df.columns:
            df["period"] = pd.to_datetime(df["period"], format="%Y-%m")

        _validate_columns(df, table_name, config["query"])
        data[table_name] = df

    return data


def load_gold_data():
    """
    Load all five Gold-layer views from DuckDB into a dict of DataFrames.

    Gold views are pre-aggregated and business-ready.  They are useful for
    displaying summary KPIs directly from SQL without applying pandas filters.

    Returns:
        dict with keys:
            "monthly_kpis"           — monthly claim counts, collection rates, etc.
            "payer_performance"      — per-payer denial rates and collection rates
            "department_performance" — per-department encounter counts and revenue
            "ar_aging"               — outstanding balance by aging bucket
            "denial_analysis"        — denial counts and appeal success by reason code
    """
    if not has_medallion_schema():
        initialize_database()

    return {
        "monthly_kpis": query_to_dataframe("SELECT * FROM gold_monthly_kpis"),
        "payer_performance": query_to_dataframe("SELECT * FROM gold_payer_performance"),
        "department_performance": query_to_dataframe("SELECT * FROM gold_department_performance"),
        "ar_aging": query_to_dataframe("SELECT * FROM gold_ar_aging"),
        "denial_analysis": query_to_dataframe("SELECT * FROM gold_denial_analysis"),
    }
