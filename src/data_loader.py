"""Load all RCM data files into pandas DataFrames."""

import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

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
    """Load all CSV files and return a dict of DataFrames.

    Raises:
        FileNotFoundError: If the data directory or a required CSV is missing.
        ValueError: If a CSV is missing required columns or cannot be parsed.
        RuntimeError: If any file fails to load for an unexpected reason.
    """
    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(
            f"Data directory not found: '{DATA_DIR}'. "
            "Run 'python generate_sample_data.py' to create the sample data."
        )

    files = {
        "payers": "payers.csv",
        "patients": "patients.csv",
        "providers": "providers.csv",
        "encounters": "encounters.csv",
        "charges": "charges.csv",
        "claims": "claims.csv",
        "payments": "payments.csv",
        "denials": "denials.csv",
        "adjustments": "adjustments.csv",
        "operating_costs": "operating_costs.csv",
    }

    data = {}
    for key, filename in files.items():
        path = os.path.join(DATA_DIR, filename)

        if not os.path.isfile(path):
            raise FileNotFoundError(
                f"Required data file not found: '{path}'. "
                "Run 'python generate_sample_data.py' to regenerate."
            )

        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            raise ValueError(f"Data file is empty: '{path}'")
        except pd.errors.ParserError as e:
            raise ValueError(f"Could not parse '{path}': {e}")

        if df.empty:
            raise ValueError(f"Data file contains no rows: '{path}'")

        _validate_columns(df, key, path)

        # Parse date columns
        date_cols = [c for c in df.columns if "date" in c.lower() or c == "period"]
        for col in date_cols:
            try:
                if col == "period":
                    df[col] = pd.to_datetime(df[col], format="%Y-%m")
                else:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception as e:
                raise ValueError(f"Failed to parse date column '{col}' in '{path}': {e}")

        # Parse boolean columns
        bool_cols = [c for c in df.columns if c.startswith("is_")]
        for col in bool_cols:
            df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})

        data[key] = df

    return data
