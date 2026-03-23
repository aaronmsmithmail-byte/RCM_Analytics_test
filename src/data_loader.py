"""
Data Loader for Healthcare RCM Analytics Dashboard
===================================================

This module is the bridge between the SQLite database and the Streamlit dashboard.
It loads all RCM data tables into pandas DataFrames, handling date parsing and
type conversions so that the metrics engine can work with clean, typed data.

Data Flow:
    CSV Files  -->  SQLite Database  -->  DataFrames (this module)  -->  Metrics Engine
                    (src/database.py)     (src/data_loader.py)          (src/metrics.py)

Why load into DataFrames instead of querying SQL directly in the metrics?
    - pandas provides powerful groupby/pivot/merge operations that would be
      verbose in SQL.
    - The metrics engine uses numpy for vectorized calculations.
    - With Streamlit's @st.cache_data decorator, the DataFrames are cached in
      memory, so subsequent page loads are instant.
    - For our data volume (~3,000 encounters), loading everything into memory
      is fast and allows the interactive sidebar filters to work instantly.

Scaling Considerations:
    If your data grows to millions of rows, you can:
    1. Push more aggregation into SQL queries (pre-aggregate by month).
    2. Use query_to_dataframe() from src/database.py for filtered queries.
    3. Partition the database by date range.
    4. Migrate to PostgreSQL or DuckDB for larger-scale analytics.

Usage:
    from src.data_loader import load_all_data
    data = load_all_data()
    # data is a dict: {"claims": DataFrame, "payments": DataFrame, ...}
"""

import os
import pandas as pd
from src.database import DB_PATH, query_to_dataframe, initialize_database


def _parse_dates(df, date_columns):
    """
    Convert string date columns to pandas Timestamp objects.

    SQLite stores dates as TEXT strings (e.g., "2024-06-15"). We need to
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
    Convert integer boolean columns from SQLite (0/1) to Python booleans.

    SQLite doesn't have a native BOOLEAN type, so we store booleans as
    INTEGER (0 = False, 1 = True). The metrics engine expects Python
    booleans so that .sum() counts True values correctly.

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

# Required columns for each table — used for validation after load
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
    """Raise ValueError if any required columns are missing."""
    required = REQUIRED_COLUMNS.get(key, [])
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Data file '{path}' is missing required columns: {missing}"
        )


def load_all_data():
    """
    Load all 10 RCM data tables from SQLite into a dictionary of DataFrames.

    This is the primary function called by the Streamlit dashboard at startup.
    It queries each table, parses dates and booleans, and returns everything
    in a single dictionary for easy access.

    Returns:
        dict: Keys are table names, values are pandas DataFrames.
            {
                "payers":          DataFrame (10 rows),
                "patients":        DataFrame (500 rows),
                "providers":       DataFrame (25 rows),
                "encounters":      DataFrame (3,000 rows),
                "charges":         DataFrame (~5,900 rows),
                "claims":          DataFrame (2,800 rows),
                "payments":        DataFrame (~2,700 rows),
                "denials":         DataFrame (~400 rows),
                "adjustments":     DataFrame (600 rows),
                "operating_costs": DataFrame (24 rows),
            }

    Auto-initialization:
        If the database file doesn't exist yet, this function automatically
        calls initialize_database() to create it from the CSV files. This
        means the dashboard "just works" on first run without manual setup.
    """
    # -----------------------------------------------------------------------
    # Auto-initialize the database if it doesn't exist.
    # This provides a seamless first-run experience: the user just runs
    # `streamlit run app.py` and everything sets itself up.
    # -----------------------------------------------------------------------
    if not os.path.exists(DB_PATH):
        print("Database not found. Initializing from CSV files...")
        initialize_database()

    # -----------------------------------------------------------------------
    # Define which columns need date parsing for each table.
    # We explicitly list them rather than auto-detecting because:
    #   1. It's faster (no need to scan column names).
    #   2. It's safer (won't accidentally parse a non-date column).
    #   3. It documents which columns are dates for future developers.
    # -----------------------------------------------------------------------
    table_config = {
        "payers": {
            "query": "SELECT * FROM payers",
            "date_cols": [],
            "bool_cols": [],
        },
        "patients": {
            "query": "SELECT * FROM patients",
            "date_cols": ["date_of_birth"],
            "bool_cols": [],
        },
        "providers": {
            "query": "SELECT * FROM providers",
            "date_cols": [],
            "bool_cols": [],
        },
        "encounters": {
            "query": "SELECT * FROM encounters",
            "date_cols": ["date_of_service", "discharge_date"],
            "bool_cols": [],
        },
        "charges": {
            "query": "SELECT * FROM charges",
            "date_cols": ["service_date", "post_date"],
            "bool_cols": [],
        },
        "claims": {
            "query": "SELECT * FROM claims",
            "date_cols": ["date_of_service", "submission_date"],
            "bool_cols": ["is_clean_claim"],
        },
        "payments": {
            "query": "SELECT * FROM payments",
            "date_cols": ["payment_date"],
            "bool_cols": ["is_accurate_payment"],
        },
        "denials": {
            "query": "SELECT * FROM denials",
            "date_cols": ["denial_date", "appeal_date"],
            "bool_cols": [],
        },
        "adjustments": {
            "query": "SELECT * FROM adjustments",
            "date_cols": ["adjustment_date"],
            "bool_cols": [],
        },
        "operating_costs": {
            "query": "SELECT * FROM operating_costs",
            "date_cols": [],  # 'period' is handled specially below
            "bool_cols": [],
        },
    }

    # -----------------------------------------------------------------------
    # Load each table into a DataFrame
    # -----------------------------------------------------------------------
    data = {}
    for table_name, config in table_config.items():
        # Execute the SQL query and get a DataFrame
        df = query_to_dataframe(config["query"])

        # Convert date strings to pandas Timestamp objects
        _parse_dates(df, config["date_cols"])

        # Convert 0/1 integers to Python booleans
        _parse_booleans(df, config["bool_cols"])

        # Special handling for the 'period' column in operating_costs.
        # It stores months as "YYYY-MM" strings, which we parse into
        # datetime objects (set to the 1st of each month).
        if table_name == "operating_costs" and "period" in df.columns:
            df["period"] = pd.to_datetime(df["period"], format="%Y-%m")

        _validate_columns(df, table_name, config["query"])
        data[table_name] = df

    return data
