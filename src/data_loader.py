"""Load all RCM data files into pandas DataFrames."""

import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def load_all_data():
    """Load all CSV files and return a dict of DataFrames."""
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
        df = pd.read_csv(path)
        # Parse date columns
        date_cols = [c for c in df.columns if "date" in c.lower() or c == "period"]
        for col in date_cols:
            if col == "period":
                df[col] = pd.to_datetime(df[col], format="%Y-%m")
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        # Parse boolean columns
        bool_cols = [c for c in df.columns if c.startswith("is_")]
        for col in bool_cols:
            df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})
        data[key] = df

    return data
