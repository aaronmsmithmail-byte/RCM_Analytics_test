"""
Database Setup and Management for Healthcare RCM Analytics
==========================================================

This module implements a Medallion Architecture (Bronze → Silver → Gold) using DuckDB
as the storage engine for the RCM Analytics application.

Medallion Architecture Overview
--------------------------------
The medallion pattern organises data into three progressively refined layers:

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  BRONZE  │  Raw ingestion layer. Data lands here exactly as it arrived   │
  │          │  from the source (CSV). All columns are TEXT. A _loaded_at    │
  │          │  timestamp records when each batch was ingested.              │
  ├──────────────────────────────────────────────────────────────────────────┤
  │  SILVER  │  Cleaned & conformed layer. An ETL step casts columns to the  │
  │          │  correct types (REAL for money, INTEGER for booleans),        │
  │          │  enforces foreign-key constraints, and filters out bad rows.  │
  ├──────────────────────────────────────────────────────────────────────────┤
  │  GOLD    │  Aggregated, business-ready layer. SQL VIEWs pre-join and     │
  │          │  aggregate Silver tables into the five key KPI domains used   │
  │          │  by the dashboard.  Gold views are always current because     │
  │          │  they are computed at query time against Silver.              │
  └──────────────────────────────────────────────────────────────────────────┘

Data flow:
    CSV files → bronze_* tables → (ETL) → silver_* tables → gold_* views → Dashboard

Table naming convention:
    bronze_payers, bronze_claims, ...   (10 bronze tables)
    silver_payers, silver_claims, ...   (10 silver tables)
    gold_monthly_kpis, gold_payer_performance, ... (5 gold views)

Why DuckDB?
    - Columnar, OLAP-optimized engine — same architecture as Snowflake.
    - Single portable file; zero server configuration.
    - Vectorized execution for fast analytical queries (GROUP BY, aggregations).
    - Full SQL support including window functions, CTEs, and date arithmetic.

Usage:
    # One-time / refresh setup
    python -m src.database

    # Programmatic
    from src.database import initialize_database
    initialize_database()
"""

import os

import duckdb
import pandas as pd

# Load .env BEFORE computing path constants so RCM_DB_PATH / RCM_DATA_DIR
# overrides are picked up even when database.py is the first module imported.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
# Defaults are derived from this file's location so the project is
# self-contained and portable.  Override via environment variables when
# the database or data files live outside the project directory (e.g.
# Docker volume mounts).  See .env.example for full documentation.
_BASE_DIR         = os.path.dirname(os.path.dirname(__file__))
_DEFAULT_DATA_DIR = os.path.join(_BASE_DIR, "data")
_DEFAULT_DB_PATH  = os.path.join(_DEFAULT_DATA_DIR, "rcm_analytics.db")

DATA_DIR = os.environ.get("RCM_DATA_DIR", _DEFAULT_DATA_DIR)
DB_PATH  = os.environ.get("RCM_DB_PATH",  _DEFAULT_DB_PATH)


# ===========================================================================
# BRONZE LAYER — Raw Ingestion Schema
# ===========================================================================
# All columns are TEXT.  The _loaded_at column records ingestion time.
# No foreign-key constraints.  Data is stored exactly as it arrived.
# ===========================================================================

BRONZE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bronze_payers (
    payer_id              TEXT,
    payer_name            TEXT,
    payer_type            TEXT,
    avg_reimbursement_pct TEXT,
    contract_id           TEXT,
    _loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_patients (
    patient_id       TEXT,
    first_name       TEXT,
    last_name        TEXT,
    date_of_birth    TEXT,
    gender           TEXT,
    primary_payer_id TEXT,
    member_id        TEXT,
    zip_code         TEXT,
    _loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_providers (
    provider_id   TEXT,
    provider_name TEXT,
    npi           TEXT,
    department    TEXT,
    specialty     TEXT,
    _loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_encounters (
    encounter_id   TEXT,
    patient_id     TEXT,
    provider_id    TEXT,
    date_of_service TEXT,
    discharge_date  TEXT,
    encounter_type  TEXT,
    department      TEXT,
    _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_charges (
    charge_id       TEXT,
    encounter_id    TEXT,
    cpt_code        TEXT,
    cpt_description TEXT,
    units           TEXT,
    charge_amount   TEXT,
    service_date    TEXT,
    post_date       TEXT,
    icd10_code      TEXT,
    _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_claims (
    claim_id            TEXT,
    encounter_id        TEXT,
    patient_id          TEXT,
    payer_id            TEXT,
    date_of_service     TEXT,
    submission_date     TEXT,
    total_charge_amount TEXT,
    claim_status        TEXT,
    is_clean_claim      TEXT,
    submission_method   TEXT,
    fail_reason         TEXT,
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_payments (
    payment_id          TEXT,
    claim_id            TEXT,
    payer_id            TEXT,
    payment_amount      TEXT,
    allowed_amount      TEXT,
    payment_date        TEXT,
    payment_method      TEXT,
    is_accurate_payment TEXT,
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_denials (
    denial_id                 TEXT,
    claim_id                  TEXT,
    denial_reason_code        TEXT,
    denial_reason_description TEXT,
    denial_date               TEXT,
    denied_amount             TEXT,
    appeal_status             TEXT,
    appeal_date               TEXT,
    recovered_amount          TEXT,
    _loaded_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_adjustments (
    adjustment_id               TEXT,
    claim_id                    TEXT,
    adjustment_type_code        TEXT,
    adjustment_type_description TEXT,
    adjustment_amount           TEXT,
    adjustment_date             TEXT,
    _loaded_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_operating_costs (
    period             TEXT,
    billing_staff_cost TEXT,
    software_cost      TEXT,
    outsourcing_cost   TEXT,
    supplies_overhead  TEXT,
    total_rcm_cost     TEXT,
    _loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


# ===========================================================================
# SILVER LAYER — Cleaned & Conformed Schema
# ===========================================================================
# Columns have proper types, FK constraints are enforced, and only valid rows
# from bronze land here.  This is the primary layer consumed by the dashboard.
# ===========================================================================

SILVER_SCHEMA_SQL = """
-- =========================================================================
-- SILVER: Reference Tables
-- =========================================================================

CREATE TABLE IF NOT EXISTS silver_payers (
    payer_id                TEXT PRIMARY KEY,
    payer_name              TEXT NOT NULL,
    payer_type              TEXT NOT NULL,
    avg_reimbursement_pct   REAL,
    contract_id             TEXT
);

CREATE TABLE IF NOT EXISTS silver_patients (
    patient_id       TEXT PRIMARY KEY,
    first_name       TEXT NOT NULL,
    last_name        TEXT NOT NULL,
    date_of_birth    TEXT,
    gender           TEXT,
    primary_payer_id TEXT,
    member_id        TEXT,
    zip_code         TEXT,
    FOREIGN KEY (primary_payer_id) REFERENCES silver_payers(payer_id)
);

CREATE TABLE IF NOT EXISTS silver_providers (
    provider_id    TEXT PRIMARY KEY,
    provider_name  TEXT NOT NULL,
    npi            TEXT,
    department     TEXT,
    specialty      TEXT
);

-- =========================================================================
-- SILVER: Transactional Tables
-- =========================================================================

CREATE TABLE IF NOT EXISTS silver_encounters (
    encounter_id    TEXT PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    provider_id     TEXT NOT NULL,
    date_of_service TEXT NOT NULL,
    discharge_date  TEXT,
    encounter_type  TEXT,
    department      TEXT,
    FOREIGN KEY (patient_id)  REFERENCES silver_patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES silver_providers(provider_id)
);

CREATE TABLE IF NOT EXISTS silver_charges (
    charge_id       TEXT PRIMARY KEY,
    encounter_id    TEXT NOT NULL,
    cpt_code        TEXT NOT NULL,
    cpt_description TEXT,
    units           INTEGER DEFAULT 1,
    charge_amount   REAL NOT NULL,
    service_date    TEXT NOT NULL,
    post_date       TEXT,
    icd10_code      TEXT,
    FOREIGN KEY (encounter_id) REFERENCES silver_encounters(encounter_id)
);

CREATE TABLE IF NOT EXISTS silver_claims (
    claim_id              TEXT PRIMARY KEY,
    encounter_id          TEXT NOT NULL,
    patient_id            TEXT NOT NULL,
    payer_id              TEXT NOT NULL,
    date_of_service       TEXT NOT NULL,
    submission_date       TEXT NOT NULL,
    total_charge_amount   REAL NOT NULL,
    claim_status          TEXT NOT NULL,
    is_clean_claim        INTEGER,
    submission_method     TEXT,
    fail_reason           TEXT,
    FOREIGN KEY (encounter_id) REFERENCES silver_encounters(encounter_id),
    FOREIGN KEY (patient_id)   REFERENCES silver_patients(patient_id),
    FOREIGN KEY (payer_id)     REFERENCES silver_payers(payer_id)
);

-- Pipeline metadata table — tracks last load time and row counts per domain.
-- Used by the data freshness sidebar panel in the dashboard.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    domain         TEXT PRIMARY KEY,
    last_loaded_at TEXT NOT NULL,
    row_count      INTEGER,
    source_file    TEXT
);

CREATE TABLE IF NOT EXISTS silver_payments (
    payment_id          TEXT PRIMARY KEY,
    claim_id            TEXT NOT NULL,
    payer_id            TEXT NOT NULL,
    payment_amount      REAL NOT NULL,
    allowed_amount      REAL,
    payment_date        TEXT NOT NULL,
    payment_method      TEXT,
    is_accurate_payment INTEGER,
    FOREIGN KEY (claim_id) REFERENCES silver_claims(claim_id)
);

CREATE TABLE IF NOT EXISTS silver_denials (
    denial_id                  TEXT PRIMARY KEY,
    claim_id                   TEXT NOT NULL,
    denial_reason_code         TEXT NOT NULL,
    denial_reason_description  TEXT,
    denial_date                TEXT NOT NULL,
    denied_amount              REAL NOT NULL,
    appeal_status              TEXT,
    appeal_date                TEXT,
    recovered_amount           REAL DEFAULT 0,
    FOREIGN KEY (claim_id) REFERENCES silver_claims(claim_id)
);

CREATE TABLE IF NOT EXISTS silver_adjustments (
    adjustment_id               TEXT PRIMARY KEY,
    claim_id                    TEXT NOT NULL,
    adjustment_type_code        TEXT NOT NULL,
    adjustment_type_description TEXT,
    adjustment_amount           REAL NOT NULL,
    adjustment_date             TEXT NOT NULL,
    FOREIGN KEY (claim_id) REFERENCES silver_claims(claim_id)
);

-- =========================================================================
-- SILVER: Operational Tables
-- =========================================================================

CREATE TABLE IF NOT EXISTS silver_operating_costs (
    period              TEXT PRIMARY KEY,
    billing_staff_cost  REAL,
    software_cost       REAL,
    outsourcing_cost    REAL,
    supplies_overhead   REAL,
    total_rcm_cost      REAL NOT NULL
);
"""


# ===========================================================================
# BRONZE → SILVER ETL
# ===========================================================================
# Type-cast each bronze column to its silver equivalent.
# Reference tables are inserted first to satisfy FK constraints.
# Rows with NULL primary keys are excluded.
# ===========================================================================

BRONZE_TO_SILVER_SQL = """
-- Reference tables first (no FK dependencies)
INSERT OR REPLACE INTO silver_payers
SELECT payer_id, payer_name, payer_type,
       CAST(avg_reimbursement_pct AS REAL), contract_id
FROM bronze_payers
WHERE payer_id IS NOT NULL AND payer_id != '';

INSERT OR REPLACE INTO silver_patients
SELECT patient_id, first_name, last_name, date_of_birth, gender,
       primary_payer_id, member_id, zip_code
FROM bronze_patients
WHERE patient_id IS NOT NULL AND patient_id != '';

INSERT OR REPLACE INTO silver_providers
SELECT provider_id, provider_name, npi, department, specialty
FROM bronze_providers
WHERE provider_id IS NOT NULL AND provider_id != '';

-- Transactional tables (depend on reference tables above)
INSERT OR REPLACE INTO silver_encounters
SELECT encounter_id, patient_id, provider_id, date_of_service,
       discharge_date, encounter_type, department
FROM bronze_encounters
WHERE encounter_id IS NOT NULL AND encounter_id != '';

INSERT OR REPLACE INTO silver_charges
SELECT charge_id, encounter_id, cpt_code, cpt_description,
       CAST(units AS INTEGER),
       CAST(charge_amount AS REAL),
       service_date, post_date, icd10_code
FROM bronze_charges
WHERE charge_id IS NOT NULL AND charge_id != '';

INSERT OR REPLACE INTO silver_claims
SELECT claim_id, encounter_id, patient_id, payer_id,
       date_of_service, submission_date,
       CAST(total_charge_amount AS REAL),
       claim_status,
       CASE UPPER(TRIM(is_clean_claim))
           WHEN 'TRUE'  THEN 1
           WHEN '1'     THEN 1
           WHEN 'YES'   THEN 1
           ELSE 0
       END,
       submission_method,
       NULLIF(TRIM(COALESCE(fail_reason, '')), '') AS fail_reason
FROM bronze_claims
WHERE claim_id IS NOT NULL AND claim_id != '';

INSERT OR REPLACE INTO silver_payments
SELECT payment_id, claim_id, payer_id,
       CAST(payment_amount AS REAL),
       CAST(allowed_amount AS REAL),
       payment_date, payment_method,
       CASE UPPER(TRIM(is_accurate_payment))
           WHEN 'TRUE'  THEN 1
           WHEN '1'     THEN 1
           WHEN 'YES'   THEN 1
           ELSE 0
       END
FROM bronze_payments
WHERE payment_id IS NOT NULL AND payment_id != '';

INSERT OR REPLACE INTO silver_denials
SELECT denial_id, claim_id, denial_reason_code, denial_reason_description,
       denial_date,
       CAST(denied_amount AS REAL),
       appeal_status, appeal_date,
       CAST(COALESCE(NULLIF(recovered_amount, ''), '0') AS REAL)
FROM bronze_denials
WHERE denial_id IS NOT NULL AND denial_id != '';

INSERT OR REPLACE INTO silver_adjustments
SELECT adjustment_id, claim_id, adjustment_type_code, adjustment_type_description,
       CAST(adjustment_amount AS REAL),
       adjustment_date
FROM bronze_adjustments
WHERE adjustment_id IS NOT NULL AND adjustment_id != '';

INSERT OR REPLACE INTO silver_operating_costs
SELECT period,
       CAST(billing_staff_cost AS REAL),
       CAST(software_cost      AS REAL),
       CAST(outsourcing_cost   AS REAL),
       CAST(supplies_overhead  AS REAL),
       CAST(total_rcm_cost     AS REAL)
FROM bronze_operating_costs
WHERE period IS NOT NULL AND period != '';
"""


# ===========================================================================
# GOLD LAYER — Aggregated Business-Ready Views
# ===========================================================================
# These SQL VIEWs join and aggregate Silver tables into the five KPI domains
# used by the dashboard.  Because they are VIEWs (not materialized tables)
# they are always up-to-date and require no refresh step.
# ===========================================================================

GOLD_VIEWS_SQL = """
-- Gold: Monthly KPI rollup
-- Aggregates claims, payments, and denial counts by service month.
-- Powers the trend charts in the Executive Summary and Revenue tabs.
CREATE VIEW IF NOT EXISTS gold_monthly_kpis AS
SELECT
    strftime(CAST(c.date_of_service AS DATE), '%Y-%m')                                         AS period,
    COUNT(DISTINCT c.claim_id)                                                   AS total_claims,
    SUM(c.total_charge_amount)                                                   AS total_charges,
    COALESCE(SUM(p.payment_amount), 0)                                           AS total_payments,
    SUM(CASE WHEN c.is_clean_claim = 1 THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(c.claim_id), 0)                                           AS clean_claim_rate,
    SUM(CASE WHEN c.claim_status = 'Denied' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(c.claim_id), 0)                                           AS denial_rate,
    CAST(COALESCE(SUM(p.payment_amount), 0) AS REAL)
        / NULLIF(SUM(c.total_charge_amount), 0)                                  AS gross_collection_rate
FROM silver_claims c
LEFT JOIN silver_payments p ON c.claim_id = p.claim_id
GROUP BY strftime(CAST(c.date_of_service AS DATE), '%Y-%m')
ORDER BY period;

-- Gold: Payer performance summary
-- One row per payer with total claims, charges, payments, and denial/collection rates.
-- Powers the Payer Analysis tab.
CREATE VIEW IF NOT EXISTS gold_payer_performance AS
SELECT
    py.payer_id,
    py.payer_name,
    py.payer_type,
    COUNT(DISTINCT c.claim_id)                                                   AS total_claims,
    SUM(c.total_charge_amount)                                                   AS total_charges,
    COALESCE(SUM(p.payment_amount), 0)                                           AS total_payments,
    SUM(CASE WHEN c.claim_status = 'Denied' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(c.claim_id), 0)                                           AS denial_rate,
    CAST(COALESCE(SUM(p.payment_amount), 0) AS REAL)
        / NULLIF(SUM(c.total_charge_amount), 0)                                  AS gross_collection_rate
FROM silver_payers py
LEFT JOIN silver_claims c  ON py.payer_id = c.payer_id
LEFT JOIN silver_payments p ON c.claim_id = p.claim_id
GROUP BY py.payer_id, py.payer_name, py.payer_type
ORDER BY total_payments DESC;

-- Gold: Department performance summary
-- Revenue, encounter volume, and revenue-per-encounter by clinical department.
-- Powers the Department Performance tab.
CREATE VIEW IF NOT EXISTS gold_department_performance AS
SELECT
    e.department,
    COUNT(DISTINCT e.encounter_id)                                               AS total_encounters,
    COALESCE(SUM(ch.charge_amount), 0)                                           AS total_charges,
    COALESCE(SUM(p.payment_amount), 0)                                           AS total_payments,
    CAST(COALESCE(SUM(p.payment_amount), 0) AS REAL)
        / NULLIF(COUNT(DISTINCT e.encounter_id), 0)                              AS revenue_per_encounter
FROM silver_encounters e
LEFT JOIN silver_charges  ch ON e.encounter_id = ch.encounter_id
LEFT JOIN silver_claims    c ON e.encounter_id = c.encounter_id
LEFT JOIN silver_payments  p ON c.claim_id     = p.claim_id
GROUP BY e.department
ORDER BY total_payments DESC;

-- Gold: A/R aging buckets
-- Groups open (unpaid) claims by age of the date-of-service.
-- Powers the A/R Aging & Cash Flow tab.
CREATE VIEW IF NOT EXISTS gold_ar_aging AS
SELECT
    CASE
        WHEN date_diff('day', CAST(c.date_of_service AS DATE), CURRENT_DATE) <=  30 THEN '0-30 days'
        WHEN date_diff('day', CAST(c.date_of_service AS DATE), CURRENT_DATE) <=  60 THEN '31-60 days'
        WHEN date_diff('day', CAST(c.date_of_service AS DATE), CURRENT_DATE) <=  90 THEN '61-90 days'
        WHEN date_diff('day', CAST(c.date_of_service AS DATE), CURRENT_DATE) <= 120 THEN '91-120 days'
        ELSE '120+ days'
    END                                                                          AS aging_bucket,
    COUNT(c.claim_id)                                                            AS claim_count,
    SUM(c.total_charge_amount)                                                   AS total_billed,
    COALESCE(SUM(p.payment_amount), 0)                                           AS total_collected,
    SUM(c.total_charge_amount) - COALESCE(SUM(p.payment_amount), 0)             AS outstanding_balance
FROM silver_claims c
LEFT JOIN silver_payments p ON c.claim_id = p.claim_id
WHERE c.claim_status NOT IN ('Paid')
GROUP BY aging_bucket;

-- Gold: Denial reason analysis
-- Aggregates denial counts, denied/recovered dollars, and appeal success rates
-- by denial reason code.  Powers the Claims & Denials tab.
CREATE VIEW IF NOT EXISTS gold_denial_analysis AS
SELECT
    d.denial_reason_code,
    d.denial_reason_description,
    COUNT(d.denial_id)                                                           AS denial_count,
    SUM(d.denied_amount)                                                         AS total_denied,
    COALESCE(SUM(d.recovered_amount), 0)                                         AS total_recovered,
    CAST(SUM(CASE WHEN d.appeal_status = 'Won' THEN 1 ELSE 0 END) AS REAL)
        / NULLIF(COUNT(d.denial_id), 0)                                          AS appeal_success_rate
FROM silver_denials d
GROUP BY d.denial_reason_code, d.denial_reason_description
ORDER BY denial_count DESC;
"""


# ===========================================================================
# Silver Layer Indexes
# ===========================================================================

INDEX_SQL = """
-- silver_claims: most-queried table in the dashboard
CREATE INDEX IF NOT EXISTS idx_silver_claims_dos         ON silver_claims(date_of_service);
CREATE INDEX IF NOT EXISTS idx_silver_claims_submission  ON silver_claims(submission_date);
CREATE INDEX IF NOT EXISTS idx_silver_claims_payer       ON silver_claims(payer_id);
CREATE INDEX IF NOT EXISTS idx_silver_claims_patient     ON silver_claims(patient_id);
CREATE INDEX IF NOT EXISTS idx_silver_claims_encounter   ON silver_claims(encounter_id);
CREATE INDEX IF NOT EXISTS idx_silver_claims_status      ON silver_claims(claim_status);

-- silver_payments: frequently joined with claims
CREATE INDEX IF NOT EXISTS idx_silver_payments_claim     ON silver_payments(claim_id);
CREATE INDEX IF NOT EXISTS idx_silver_payments_date      ON silver_payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_silver_payments_payer     ON silver_payments(payer_id);

-- silver_denials: denial analysis tab
CREATE INDEX IF NOT EXISTS idx_silver_denials_claim      ON silver_denials(claim_id);
CREATE INDEX IF NOT EXISTS idx_silver_denials_reason     ON silver_denials(denial_reason_code);
CREATE INDEX IF NOT EXISTS idx_silver_denials_date       ON silver_denials(denial_date);

-- silver_adjustments
CREATE INDEX IF NOT EXISTS idx_silver_adjustments_claim  ON silver_adjustments(claim_id);
CREATE INDEX IF NOT EXISTS idx_silver_adjustments_type   ON silver_adjustments(adjustment_type_code);

-- silver_encounters: filtered by date, department, type
CREATE INDEX IF NOT EXISTS idx_silver_encounters_dos     ON silver_encounters(date_of_service);
CREATE INDEX IF NOT EXISTS idx_silver_encounters_dept    ON silver_encounters(department);
CREATE INDEX IF NOT EXISTS idx_silver_encounters_type    ON silver_encounters(encounter_type);
CREATE INDEX IF NOT EXISTS idx_silver_encounters_patient ON silver_encounters(patient_id);

-- silver_charges
CREATE INDEX IF NOT EXISTS idx_silver_charges_encounter  ON silver_charges(encounter_id);
CREATE INDEX IF NOT EXISTS idx_silver_charges_date       ON silver_charges(service_date);
"""

# ===========================================================================
# METADATA LAYER — AI-queryable semantic and knowledge graph tables
# ===========================================================================
# These four tables persist the business metadata that an AI app needs to
# understand the data model and generate correct SQL queries:
#
#   meta_kpi_catalog     — 23 KPI definitions with formulas and benchmarks
#   meta_semantic_layer  — Business concept → KPI → silver column mappings
#   meta_kg_nodes        — Entity nodes (tables) with column descriptions
#   meta_kg_edges        — Foreign-key relationships between entities
#
# All four tables are populated by persist_metadata() on every initialisation.
# They are safe to query directly from an AI agent alongside the Silver layer.
# ===========================================================================

METADATA_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta_kpi_catalog (
    metric_name   TEXT PRIMARY KEY,
    category      TEXT NOT NULL,
    definition    TEXT,
    formula       TEXT,
    data_sources  TEXT,
    dashboard_tab TEXT,
    benchmark     TEXT
);

CREATE SEQUENCE IF NOT EXISTS seq_semantic_layer START 1;
CREATE TABLE IF NOT EXISTS meta_semantic_layer (
    id               INTEGER PRIMARY KEY DEFAULT nextval('seq_semantic_layer'),
    business_concept TEXT NOT NULL,
    kpi_name         TEXT NOT NULL,
    silver_columns   TEXT,
    formula          TEXT,
    business_rule    TEXT
);

CREATE TABLE IF NOT EXISTS meta_kg_nodes (
    entity_id     TEXT PRIMARY KEY,
    entity_name   TEXT NOT NULL,
    entity_group  TEXT,
    silver_table  TEXT,
    description   TEXT,
    source_system TEXT
);

CREATE SEQUENCE IF NOT EXISTS seq_kg_edges START 1;
CREATE TABLE IF NOT EXISTS meta_kg_edges (
    id               INTEGER PRIMARY KEY DEFAULT nextval('seq_kg_edges'),
    parent_entity    TEXT NOT NULL,
    child_entity     TEXT NOT NULL,
    join_column      TEXT,
    cardinality      TEXT,
    business_meaning TEXT
);
"""

# ---------------------------------------------------------------------------
# Legacy table names from the pre-medallion schema.
# initialize_database() drops these before creating the new schema so that
# existing databases migrate cleanly without manual intervention.
# ---------------------------------------------------------------------------
_LEGACY_TABLES = [
    "adjustments", "denials", "payments", "claims", "charges",
    "encounters", "patients", "providers", "operating_costs", "payers",
]


def get_connection(db_path=None, read_only=False):
    """
    Create and return a DuckDB database connection.

    Args:
        db_path: Optional override for the database file path.
                 Defaults to ./data/rcm_analytics.db.
        read_only: If True, open the database in read-only mode.
                   Use this to avoid lock conflicts when another
                   process (e.g. Cube) holds a write lock.
                   Falls back to read-write if the file does not exist.

    Returns:
        duckdb.DuckDBPyConnection.
    """
    path = db_path or DB_PATH
    # Can't open a non-existent file in read-only mode
    if read_only and not os.path.exists(path):
        read_only = False
    conn = duckdb.connect(path, read_only=read_only)
    return conn


def create_tables(conn):
    """
    Create all Bronze, Silver, and Gold schema objects.

    Idempotent — the IF NOT EXISTS / CREATE VIEW IF NOT EXISTS clauses
    mean this is safe to call on an already-initialised database.

    Args:
        conn: An active DuckDB connection.
    """
    conn.execute(BRONZE_SCHEMA_SQL)
    conn.execute(SILVER_SCHEMA_SQL)
    conn.execute(GOLD_VIEWS_SQL)
    conn.execute(INDEX_SQL)
    conn.execute(METADATA_SCHEMA_SQL)
    print("  [OK] Bronze tables, Silver tables, Gold views, indexes, and metadata tables created.")


def load_csv_to_bronze(conn, bronze_table, csv_filename):
    """
    Load a CSV file into the corresponding Bronze table.

    All values land as TEXT (raw, untyped) exactly as they appear in the CSV.
    The _loaded_at column is set automatically by the DuckDB DEFAULT expression.

    Args:
        conn:          An active DuckDB connection.
        bronze_table:  Bronze table name (e.g. "bronze_claims").
        csv_filename:  CSV filename in the data directory (e.g. "claims.csv").
    """
    csv_path = os.path.join(DATA_DIR, csv_filename)
    if not os.path.exists(csv_path):
        print(f"  [SKIP] {csv_filename} not found at {csv_path}")
        return

    # Read all columns as strings — bronze stores raw TEXT
    df = pd.read_csv(csv_path, dtype=str)

    # Strip the metadata column if it happens to be in the CSV already
    df = df.drop(columns=["_loaded_at"], errors="ignore")

    conn.execute(f"DELETE FROM {bronze_table}")
    # DuckDB can INSERT directly from a pandas DataFrame variable
    cols = ", ".join(df.columns)
    conn.execute(f"INSERT INTO {bronze_table}({cols}) SELECT {cols} FROM df")

    # Record load event in pipeline_runs for data freshness tracking.
    domain = bronze_table.replace("bronze_", "")
    conn.execute(
        "INSERT OR REPLACE INTO pipeline_runs (domain, last_loaded_at, row_count, source_file) "
        "VALUES (?, CURRENT_TIMESTAMP, ?, ?)",
        [domain, len(df), csv_filename],
    )

    print(f"  [OK] Bronze: loaded {len(df):,} rows into '{bronze_table}' from {csv_filename}")


def _etl_bronze_to_silver(conn):
    """
    Transform Bronze (raw TEXT) data into the Silver (typed, validated) layer.

    Runs the BRONZE_TO_SILVER_SQL script which:
      - Casts money columns to REAL
      - Converts boolean strings ('True'/'False') to INTEGER (1/0)
      - Casts integer columns appropriately
      - Skips rows with NULL/empty primary keys

    Rows that violate constraints are silently skipped by INSERT OR REPLACE.
    """
    conn.execute(BRONZE_TO_SILVER_SQL)
    print("  [OK] Silver: ETL from Bronze complete.")


def persist_metadata(conn):
    """
    Populate the four meta_* tables from the module-level metadata constants
    defined in src.metadata_pages.

    This is called once per database initialisation so an AI app can query
    the metadata tables directly alongside the Silver layer without needing
    to parse Python source files.

    Tables populated:
        meta_kpi_catalog     — KPI name, category, formula, benchmark
        meta_semantic_layer  — business concept → KPI → silver columns
        meta_kg_nodes        — entity nodes with silver table descriptions
        meta_kg_edges        — FK relationships between entities

    Args:
        conn: An active DuckDB connection.
    """
    from src.metadata_pages import (
        _KG_NODES,
        _KG_RELATIONSHIPS,
        _KPI_CATALOG,
        _SEMANTIC_LAYER,
    )

    # ── meta_kpi_catalog ───────────────────────────────────────────────
    conn.execute("DELETE FROM meta_kpi_catalog;")
    # Build a benchmark lookup from the README benchmarks encoded per metric
    _BENCHMARKS = {
        "Days in A/R (DAR)":              "≤ 35 days",
        "Net Collection Rate (NCR)":      "≥ 95%",
        "Gross Collection Rate (GCR)":    "≥ 70%",
        "Clean Claim Rate":               "≥ 90%",
        "Denial Rate":                    "≤ 10%",
        "First-Pass Resolution Rate":     "≥ 85%",
        "Payment Accuracy Rate":          "≥ 95%",
        "Bad Debt Rate":                  "≤ 3%",
        "Cost to Collect":                "≤ 3%",
    }
    for kpi in _KPI_CATALOG:
        conn.execute(
            "INSERT OR REPLACE INTO meta_kpi_catalog "
            "(metric_name, category, definition, formula, data_sources, dashboard_tab, benchmark) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                kpi["Metric"],
                kpi["Category"],
                kpi["Definition"],
                kpi["Formula"],
                kpi["Data Sources"],
                kpi["Dashboard Tab"],
                _BENCHMARKS.get(kpi["Metric"]),
            ),
        )

    # ── meta_semantic_layer ────────────────────────────────────────────
    conn.execute("DELETE FROM meta_semantic_layer")
    for row in _SEMANTIC_LAYER:
        conn.execute(
            "INSERT INTO meta_semantic_layer "
            "(business_concept, kpi_name, silver_columns, formula, business_rule) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                row["business_concept"],
                row["kpi_name"],
                row["silver_columns"],
                row["formula"],
                row["business_rule"],
            ),
        )

    # ── meta_kg_nodes ──────────────────────────────────────────────────
    conn.execute("DELETE FROM meta_kg_nodes")
    for node in _KG_NODES:
        # Derive silver_table name from the id (operating_costs → silver_operating_costs)
        silver_table = f"silver_{node['id']}"
        conn.execute(
            "INSERT OR REPLACE INTO meta_kg_nodes "
            "(entity_id, entity_name, entity_group, silver_table, description, source_system) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                node["id"],
                node["label"].replace("\n", " "),
                node["group"],
                silver_table,
                node["hover"],
                node.get("source_system", ""),
            ),
        )

    # ── meta_kg_edges ──────────────────────────────────────────────────
    conn.execute("DELETE FROM meta_kg_edges")
    for rel in _KG_RELATIONSHIPS:
        conn.execute(
            "INSERT INTO meta_kg_edges "
            "(parent_entity, child_entity, join_column, cardinality, business_meaning) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                rel["parent_table"],
                rel["child_table"],
                rel["join_column"],
                rel["cardinality"],
                rel["business_meaning"],
            ),
        )

    conn.commit()
    print("  [OK] Metadata tables populated (meta_kpi_catalog, meta_semantic_layer, meta_kg_nodes, meta_kg_edges).")


def initialize_database(db_path=None):
    """
    Full medallion-architecture database initialisation.

    Steps:
        1. Open / create the DuckDB database file.
        2. Drop any legacy un-prefixed tables (migration from old schema).
        3. Create Bronze tables, Silver tables, and Gold views.
        4. Load each CSV into its Bronze table (raw TEXT ingestion).
        5. ETL Bronze → Silver (type casting + validation).
        6. Report row counts for all three layers.

    Args:
        db_path: Optional path override (defaults to ./data/rcm_analytics.db).
    """
    print("=" * 60)
    print("Healthcare RCM Analytics — Medallion Architecture Init")
    print("=" * 60)

    conn = get_connection(db_path)
    print(f"\n  Database: {db_path or DB_PATH}\n")

    # ------------------------------------------------------------------
    # Step 1: Migrate legacy schema
    # ------------------------------------------------------------------
    print("Step 1: Removing legacy (pre-medallion) tables if present...")
    for tbl in _LEGACY_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
    # Add source_system column to meta_kg_nodes if it was created before this
    # column existed (existing databases won't have it from CREATE TABLE alone).
    try:
        conn.execute("ALTER TABLE meta_kg_nodes ADD COLUMN source_system TEXT")
    except Exception:
        pass  # Column already exists — safe to ignore
    print("  [OK] Legacy tables cleared.\n")

    # ------------------------------------------------------------------
    # Step 1b: Schema version migration
    # Drop Silver tables and Gold views if the schema is outdated (e.g.
    # fail_reason column added in a later version).  Bronze tables are
    # always safe to keep — they are truncated and reloaded from CSV.
    # ------------------------------------------------------------------
    _silver_tables = [
        "silver_claims", "silver_payments", "silver_denials", "silver_adjustments",
        "silver_encounters", "silver_charges", "silver_payers", "silver_patients",
        "silver_providers", "silver_operating_costs",
    ]
    _gold_views = [
        "gold_monthly_kpis", "gold_payer_performance", "gold_department_performance",
        "gold_ar_aging", "gold_denial_analysis",
    ]
    try:
        # Check bronze_claims (not silver) because a previous partial run may
        # have already rebuilt silver_claims with the new schema while leaving
        # bronze_claims on the old schema (missing fail_reason).
        cur = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'bronze_claims'"
        )
        existing_cols = {row[0] for row in cur.fetchall()}
        if existing_cols and "fail_reason" not in existing_cols:
            print("Step 1b: Schema migration — removing outdated Bronze/Silver/Gold objects...")
            for tbl in _silver_tables:
                conn.execute(f"DROP TABLE IF EXISTS {tbl}")
            for vw in _gold_views:
                conn.execute(f"DROP VIEW IF EXISTS {vw}")
            # Bronze tables are always reloaded from CSV — safe to drop for clean rebuild
            _bronze_tables = ["bronze_" + t.replace("silver_", "") for t in _silver_tables]
            for tbl in _bronze_tables:
                conn.execute(f"DROP TABLE IF EXISTS {tbl}")
            conn.execute("DROP TABLE IF EXISTS pipeline_runs")
            print("  [OK] Migration complete — all layers will be rebuilt.\n")
    except Exception:
        pass  # table doesn't exist yet; normal first-run

    # ------------------------------------------------------------------
    # Step 2: Create schema (Bronze + Silver + Gold)
    # ------------------------------------------------------------------
    print("Step 2: Creating Bronze tables, Silver tables, and Gold views...")
    create_tables(conn)
    print()

    # ------------------------------------------------------------------
    # Step 3: Ingest CSV files into Bronze (raw TEXT)
    # ------------------------------------------------------------------
    print("Step 3: Loading CSV files into Bronze layer...")
    bronze_csv_map = [
        ("bronze_payers",          "payers.csv"),
        ("bronze_patients",        "patients.csv"),
        ("bronze_providers",       "providers.csv"),
        ("bronze_encounters",      "encounters.csv"),
        ("bronze_charges",         "charges.csv"),
        ("bronze_claims",          "claims.csv"),
        ("bronze_payments",        "payments.csv"),
        ("bronze_denials",         "denials.csv"),
        ("bronze_adjustments",     "adjustments.csv"),
        ("bronze_operating_costs", "operating_costs.csv"),
    ]
    for bronze_table, csv_filename in bronze_csv_map:
        load_csv_to_bronze(conn, bronze_table, csv_filename)
    print()

    # ------------------------------------------------------------------
    # Step 4: ETL Bronze → Silver
    # ------------------------------------------------------------------
    print("Step 4: Running ETL — Bronze → Silver (type casting & validation)...")
    _etl_bronze_to_silver(conn)
    print()

    # ------------------------------------------------------------------
    # Step 4b: Persist metadata (AI-queryable semantic + KG tables)
    # ------------------------------------------------------------------
    print("Step 4b: Persisting metadata layer...")
    persist_metadata(conn)
    print()

    # ------------------------------------------------------------------
    # Step 4c: Seed Neo4j knowledge graph (if available)
    # ------------------------------------------------------------------
    try:
        from src.neo4j_client import is_neo4j_available, seed_knowledge_graph
        if is_neo4j_available():
            print("Step 4c: Seeding Neo4j knowledge graph...")
            if seed_knowledge_graph():
                print("  [OK] Neo4j knowledge graph seeded.\n")
            else:
                print("  [WARN] Neo4j seeding failed — using DuckDB fallback.\n")
        else:
            print("Step 4c: Neo4j not available — skipping (DuckDB fallback active).\n")
    except ImportError:
        print("Step 4c: Neo4j driver not installed — skipping.\n")

    # ------------------------------------------------------------------
    # Step 5: Verify row counts across all layers
    # ------------------------------------------------------------------
    print("Step 5: Row count verification")
    print(f"  {'Table':<35} {'Bronze':>8} {'Silver':>8}")
    print(f"  {'-'*35} {'-'*8} {'-'*8}")
    table_names = [
        ("payers", "payers"),
        ("patients", "patients"),
        ("providers", "providers"),
        ("encounters", "encounters"),
        ("charges", "charges"),
        ("claims", "claims"),
        ("payments", "payments"),
        ("denials", "denials"),
        ("adjustments", "adjustments"),
        ("operating_costs", "operating_costs"),
    ]
    for base, _ in table_names:
        b = conn.execute(f"SELECT COUNT(*) FROM bronze_{base}").fetchone()[0]
        s = conn.execute(f"SELECT COUNT(*) FROM silver_{base}").fetchone()[0]
        print(f"  {base:<35} {b:>8,} {s:>8,}")

    print("\n  Gold views (computed at query time — no row count):")
    gold_views = [
        "gold_monthly_kpis",
        "gold_payer_performance",
        "gold_department_performance",
        "gold_ar_aging",
        "gold_denial_analysis",
    ]
    for v in gold_views:
        print(f"    ✓ {v}")

    conn.close()
    print("\n" + "=" * 60)
    print("Medallion architecture initialisation complete!")
    print(f"Database: {db_path or DB_PATH}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Utility helpers (used by data_loader and metadata pages)
# ---------------------------------------------------------------------------

def query_to_dataframe(sql, params=None, db_path=None):
    """
    Execute a SQL query and return results as a pandas DataFrame.

    Args:
        sql:     SQL string (may contain $1/$2 or ? placeholders).
        params:  Optional tuple/list of parameters for the query.
        db_path: Optional path override.

    Returns:
        pd.DataFrame with query results.
    """
    conn = get_connection(db_path, read_only=True)
    try:
        if params:
            return conn.execute(sql, params).df()
        else:
            return conn.execute(sql).df()
    finally:
        conn.close()


def get_table_info(table_name, db_path=None):
    """
    Return column metadata for a named table (useful for debugging).

    Args:
        table_name: Name of the table to inspect (use full name, e.g. 'silver_claims').
        db_path:    Optional path override.

    Returns:
        list of tuples: (column_name, data_type, is_nullable, key, default, extra).
    """
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(f"DESCRIBE {table_name}")
        return cursor.fetchall()
    finally:
        conn.close()


def build_filter_cte(start_date, end_date, payer_id=None,
                     department=None, encounter_type=None):
    """
    Return a (cte_sql, params) pair for the filtered_claims CTE.

    The CTE joins silver_claims to silver_encounters so that all four
    filter dimensions (date, payer, department, encounter type) can be
    applied in a single WHERE clause.  All metric query functions use this
    as their base SQL construct.

    Args:
        start_date:     'YYYY-MM-DD' lower bound for date_of_service (inclusive).
        end_date:       'YYYY-MM-DD' upper bound for date_of_service (inclusive).
        payer_id:       Optional payer_id to filter claims.
        department:     Optional department name from silver_encounters.
        encounter_type: Optional encounter_type from silver_encounters.

    Returns:
        tuple: (cte_sql, params)
            cte_sql — SQL string starting with 'WITH filtered_claims AS ('.
            params  — list of positional parameters to bind.
    """
    clauses = ["c.date_of_service BETWEEN ? AND ?"]
    params = [start_date, end_date]
    if payer_id:
        clauses.append("c.payer_id = ?")
        params.append(payer_id)
    if department:
        clauses.append("e.department = ?")
        params.append(department)
    if encounter_type:
        clauses.append("e.encounter_type = ?")
        params.append(encounter_type)

    where_clause = " AND ".join(clauses)
    cte_sql = f"""WITH filtered_claims AS (
    SELECT c.*
    FROM silver_claims c
    LEFT JOIN silver_encounters e ON c.encounter_id = e.encounter_id
    WHERE {where_clause}
)
"""
    return cte_sql, params


def has_medallion_schema(db_path=None):
    """
    Return True if the database already has the Silver layer populated.

    Used by data_loader to decide whether to auto-initialise.
    """
    path = db_path or DB_PATH
    if not os.path.exists(path):
        return False
    try:
        conn = duckdb.connect(path, read_only=True)
        # Check both that the table exists and that the schema is current
        # (fail_reason column was added in schema v2).
        conn.execute("SELECT fail_reason FROM silver_claims LIMIT 1")
        conn.close()
        return True
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return False


# ===========================================================================
# CLI Entry Point
# ===========================================================================

if __name__ == "__main__":
    initialize_database()
