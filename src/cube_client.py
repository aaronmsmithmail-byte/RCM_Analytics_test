"""
Cube Semantic Layer Client
===========================

Connects to a Cube REST API to query the semantic layer for:
1. Metadata — cubes, measures, dimensions, joins (for the AI system prompt
   and Semantic Layer metadata page)
2. Metric queries — execute aggregation queries through Cube instead of raw SQL

Gracefully returns None when Cube is unavailable so the app falls back
to DuckDB meta_semantic_layer and raw SQL queries.

Environment variables:
    CUBE_API_URL    — REST API base URL (default: http://localhost:4000/cubejs-api/v1)
    CUBE_API_SECRET — API secret for authentication (default: rcm_analytics_dev_secret)
"""

import json
import os
import time

import pandas as pd
import requests

CUBE_API_URL = os.environ.get("CUBE_API_URL", "http://localhost:4000/cubejs-api/v1")
CUBE_API_SECRET = os.environ.get("CUBE_API_SECRET", "rcm_analytics_dev_secret")

# TTL-cached health check
_health_cache = {"available": None, "checked_at": 0}
_HEALTH_TTL = 60  # seconds


def _headers():
    """Build request headers with Cube API authentication."""
    return {
        "Content-Type": "application/json",
        "Authorization": CUBE_API_SECRET,
    }


def is_cube_available() -> bool:
    """Check if Cube API is reachable. Result cached for 60 seconds."""
    now = time.time()
    if now - _health_cache["checked_at"] < _HEALTH_TTL and _health_cache["available"] is not None:
        return _health_cache["available"]
    try:
        resp = requests.get(f"{CUBE_API_URL}/readyz", timeout=3)
        _health_cache["available"] = resp.status_code == 200
    except Exception:
        _health_cache["available"] = False
    _health_cache["checked_at"] = now
    return _health_cache["available"]


def get_cube_meta():
    """
    Fetch semantic metadata from Cube's /meta endpoint.

    Returns a dict with keys:
        cubes: list of cube definitions (name, title, measures, dimensions, joins)
    Returns None if Cube is unavailable.
    """
    if not is_cube_available():
        return None
    try:
        resp = requests.get(f"{CUBE_API_URL}/meta", headers=_headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def get_semantic_mappings():
    """
    Extract business concept → KPI → column mappings from Cube metadata.

    Transforms Cube's /meta response into the same format used by
    meta_semantic_layer (business_concept, kpi_name, silver_columns, formula, business_rule).

    Returns:
        list of dicts matching meta_semantic_layer schema, or None if unavailable.
    """
    meta = get_cube_meta()
    if not meta or "cubes" not in meta:
        return None

    mappings = []
    for cube in meta["cubes"]:
        cube_name = cube.get("name", "")
        # Views represent business concepts; regular cubes are tables
        is_view = cube.get("type") == "view"
        concept = cube.get("title", cube_name) if is_view else cube_name.replace("_", " ").title()

        for measure in cube.get("measures", []):
            silver_cols = f"{cube_name}.{measure.get('name', '')}"
            mappings.append({
                "business_concept": concept,
                "kpi_name": measure.get("title", measure.get("name", "")),
                "silver_columns": silver_cols,
                "formula": measure.get("type", ""),
                "business_rule": measure.get("description", ""),
            })

    return mappings if mappings else None


def query_cube(measures, dimensions=None, filters=None, time_dimensions=None,
               order=None, limit=None):
    """
    Execute a Cube query and return results as a pandas DataFrame.

    Args:
        measures:        list of measure names (e.g. ["claims.total_charges", "claims.count"])
        dimensions:      list of dimension names (e.g. ["claims.period", "payers.payer_name"])
        filters:         list of filter dicts (e.g. [{"member": "claims.payer_id", "operator": "equals", "values": ["PYR001"]}])
        time_dimensions: list of time dimension dicts for date filtering
        order:           dict of ordering (e.g. {"claims.total_charges": "desc"})
        limit:           max rows to return

    Returns:
        pd.DataFrame with query results, or None if Cube is unavailable.
    """
    if not is_cube_available():
        return None

    query = {"measures": measures}
    if dimensions:
        query["dimensions"] = dimensions
    if filters:
        query["filters"] = filters
    if time_dimensions:
        query["timeDimensions"] = time_dimensions
    if order:
        query["order"] = order
    if limit:
        query["limit"] = limit

    try:
        resp = requests.get(
            f"{CUBE_API_URL}/load",
            params={"query": json.dumps(query)},
            headers=_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", [])
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        # Cube returns column names like "claims.total_charges" — simplify
        df.columns = [c.split(".")[-1] if "." in c else c for c in df.columns]
        return df
    except Exception:
        return None


def build_cube_filters(start_date, end_date, payer_id=None,
                       department=None, encounter_type=None):
    """
    Convert FilterParams into Cube filter format.

    Args:
        start_date:     'YYYY-MM-DD' lower bound
        end_date:       'YYYY-MM-DD' upper bound
        payer_id:       Optional payer_id filter
        department:     Optional department filter
        encounter_type: Optional encounter_type filter

    Returns:
        tuple: (filters, time_dimensions) for use with query_cube()
    """
    filters = []
    time_dimensions = [{
        "dimension": "claims.date_of_service",
        "dateRange": [start_date, end_date],
    }]

    if payer_id:
        filters.append({
            "member": "claims.payer_id",
            "operator": "equals",
            "values": [payer_id],
        })
    if department:
        filters.append({
            "member": "encounters.department",
            "operator": "equals",
            "values": [department],
        })
    if encounter_type:
        filters.append({
            "member": "encounters.encounter_type",
            "operator": "equals",
            "values": [encounter_type],
        })

    return filters, time_dimensions
