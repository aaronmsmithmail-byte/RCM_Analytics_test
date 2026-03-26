"""
Metadata Pages for Healthcare RCM Analytics Dashboard
======================================================

This module contains five supplemental pages accessible from the sidebar:
  - Data Catalog        : Searchable table of all 23 KPIs + 10 data tables
  - Data Lineage        : DAG showing the full pipeline from CSV to dashboard
  - Knowledge Graph     : Entity-relationship diagram of the 10 data entities
  - Semantic Layer      : Business concept → KPI → raw column mapping
  - AI Architecture     : Process diagram — how the AI chat tab uses the
                          semantic layer, knowledge graph, and SQL tool loop
                          to answer natural-language RCM questions

Each render_*() function is called from app.py based on st.session_state["active_page"].
"""

import streamlit as st
import pandas as pd
import graphviz


# ---------------------------------------------------------------------------
# Shared DB helper
# ---------------------------------------------------------------------------

def _query_meta(sql: str) -> pd.DataFrame:
    """
    Run a SELECT against the SQLite DB and return a DataFrame.

    Returns an empty DataFrame on any error so the page degrades gracefully
    if the database hasn't been initialised yet.
    """
    from src.database import get_connection
    conn = get_connection()
    try:
        return conn.execute(sql).df()
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


# ── KPI catalog data ──────────────────────────────────────────────────
# NOTE: This dict is the FALLBACK only.  render_data_catalog() queries
# meta_kpi_catalog from SQLite at render time; this list is used only
# if the database isn't available yet (e.g. first-run before ETL).

_KPI_CATALOG_FALLBACK = [
    {
        "Metric": "Days in A/R (DAR)",
        "Category": "Financial Performance",
        "Definition": "How many days of charges are sitting unpaid. The single most important cash-flow metric in RCM.",
        "Formula": "A/R Balance / Avg Daily Charges  (A/R = Cumulative Charges − Cumulative Payments; Avg Daily = Monthly Charges / 30)",
        "Data Sources": "silver_claims.total_charge_amount, silver_payments.payment_amount",
        "Dashboard Tab": "Executive Summary, A/R Aging & Cash Flow",
    },
    {
        "Metric": "Net Collection Rate (NCR)",
        "Category": "Financial Performance",
        "Definition": "Percentage of collectible revenue actually collected. Adjustments remove contractually non-collectible amounts.",
        "Formula": "Payments / (Charges − Adjustments) × 100",
        "Data Sources": "silver_payments.payment_amount, silver_claims.total_charge_amount, silver_adjustments.adjustment_amount",
        "Dashboard Tab": "Executive Summary, Collections & Revenue",
    },
    {
        "Metric": "Gross Collection Rate (GCR)",
        "Category": "Financial Performance",
        "Definition": "Total collections as a percentage of gross charges billed, before adjustments.",
        "Formula": "SUM(payments) / SUM(charges) × 100",
        "Data Sources": "silver_claims.total_charge_amount, silver_payments.payment_amount",
        "Dashboard Tab": "Executive Summary, Collections & Revenue",
    },
    {
        "Metric": "Cost to Collect",
        "Category": "Financial Performance",
        "Definition": "RCM operating cost per dollar collected — measures billing department efficiency.",
        "Formula": "Total RCM Cost / Total Collections × 100",
        "Data Sources": "silver_operating_costs.total_rcm_cost, silver_payments.payment_amount",
        "Dashboard Tab": "Executive Summary, Collections & Revenue",
    },
    {
        "Metric": "Bad Debt Rate",
        "Category": "Financial Performance",
        "Definition": "Percentage of charges written off as uncollectable bad debt.",
        "Formula": "Bad Debt Write-offs / Total Charges × 100",
        "Data Sources": "silver_adjustments.adjustment_type_code, silver_adjustments.adjustment_amount, silver_claims.total_charge_amount",
        "Dashboard Tab": "Executive Summary",
    },
    {
        "Metric": "Average Reimbursement per Encounter",
        "Category": "Financial Performance",
        "Definition": "Average payment received per patient encounter — tracks revenue per visit.",
        "Formula": "Total Payments / Number of Encounters",
        "Data Sources": "silver_payments.payment_amount, silver_encounters.encounter_id",
        "Dashboard Tab": "Executive Summary",
    },
    {
        "Metric": "Clean Claim Rate",
        "Category": "Claims Quality",
        "Definition": "Percentage of claims submitted without errors that are accepted on first pass.",
        "Formula": "Clean Claims / Total Claims × 100",
        "Data Sources": "silver_claims.is_clean_claim",
        "Dashboard Tab": "Executive Summary, Claims & Denials",
    },
    {
        "Metric": "Denial Rate",
        "Category": "Claims Quality",
        "Definition": "Percentage of submitted claims denied by payers.",
        "Formula": "Denied Claims / Total Claims × 100",
        "Data Sources": "silver_claims.claim_status",
        "Dashboard Tab": "Executive Summary, Claims & Denials",
    },
    {
        "Metric": "First-Pass Resolution Rate",
        "Category": "Claims Quality",
        "Definition": "Percentage of claims resolved (paid or denied) on first submission without rework.",
        "Formula": "Claims Resolved on First Pass / Total Claims × 100",
        "Data Sources": "silver_claims.claim_status, silver_claims.is_clean_claim",
        "Dashboard Tab": "Claims & Denials",
    },
    {
        "Metric": "Charge Lag",
        "Category": "Claims Quality",
        "Definition": "Average days between date of service and claim submission — delays increase A/R.",
        "Formula": "AVG(submission_date − date_of_service) in days",
        "Data Sources": "silver_claims.submission_date, silver_claims.date_of_service",
        "Dashboard Tab": "Claims & Denials",
    },
    {
        "Metric": "Denial Reasons",
        "Category": "Claims Quality",
        "Definition": "Distribution of denial reason codes — identifies root causes for process improvement.",
        "Formula": "COUNT(*) GROUP BY denial_reason_code",
        "Data Sources": "silver_denials.denial_reason_code, silver_denials.denial_reason_description, silver_denials.denied_amount",
        "Dashboard Tab": "Claims & Denials",
    },
    {
        "Metric": "Appeal Success Rate",
        "Category": "Recovery & Appeals",
        "Definition": "Percentage of appealed denials that are successfully overturned.",
        "Formula": "Successful Appeals / Total Appealed Denials × 100",
        "Data Sources": "silver_denials.appeal_status",
        "Dashboard Tab": "Claims & Denials",
    },
    {
        "Metric": "A/R Aging",
        "Category": "Recovery & Appeals",
        "Definition": "Dollar value of unpaid claims bucketed by age (0-30, 31-60, 61-90, 91-120, 120+ days).",
        "Formula": "SUM(outstanding_amount) GROUP BY age_bucket",
        "Data Sources": "silver_claims.date_of_service, silver_claims.total_charge_amount, silver_claims.claim_status, silver_payments.payment_amount",
        "Dashboard Tab": "A/R Aging & Cash Flow",
    },
    {
        "Metric": "Payment Accuracy Rate",
        "Category": "Recovery & Appeals",
        "Definition": "Percentage of payments received that match the contracted reimbursement amount.",
        "Formula": "Accurate Payments / Total Payments × 100",
        "Data Sources": "silver_payments.is_accurate_payment",
        "Dashboard Tab": "Executive Summary",
    },
    {
        "Metric": "Payer Mix",
        "Category": "Segmentation",
        "Definition": "Revenue distribution across payer types (Medicare, Medicaid, Commercial, Self-pay).",
        "Formula": "SUM(payments) GROUP BY payer_type",
        "Data Sources": "silver_payments.payment_amount, silver_payers.payer_type, silver_claims.payer_id",
        "Dashboard Tab": "Payer Analysis",
    },
    {
        "Metric": "Denial Rate by Payer",
        "Category": "Segmentation",
        "Definition": "Denial rate broken down per payer — identifies problematic payer relationships.",
        "Formula": "Denied Claims / Total Claims GROUP BY payer_id",
        "Data Sources": "silver_claims.claim_status, silver_claims.payer_id, silver_payers.payer_name",
        "Dashboard Tab": "Payer Analysis",
    },
    {
        "Metric": "Department Performance",
        "Category": "Segmentation",
        "Definition": "Revenue and encounter volume broken down by clinical department.",
        "Formula": "SUM(payments), COUNT(encounters) GROUP BY department",
        "Data Sources": "silver_encounters.department, silver_payments.payment_amount, silver_claims.encounter_id",
        "Dashboard Tab": "Department Performance",
    },
]

_TABLE_CATALOG = [
    # ── Bronze layer ──────────────────────────────────────────────────────
    {"Layer": "Bronze", "Table": "bronze_payers",          "Source System": "Payer Master",         "Key Columns": "payer_id (TEXT), _loaded_at",                                       "Description": "Raw CSV ingestion — insurance payer master list",                    "Relationships": "Source for silver_payers"},
    {"Layer": "Bronze", "Table": "bronze_patients",        "Source System": "EHR",                  "Key Columns": "patient_id (TEXT), primary_payer_id (TEXT), _loaded_at",              "Description": "Raw CSV ingestion — patient demographics",                          "Relationships": "Source for silver_patients"},
    {"Layer": "Bronze", "Table": "bronze_providers",       "Source System": "EHR",                  "Key Columns": "provider_id (TEXT), department (TEXT), _loaded_at",                   "Description": "Raw CSV ingestion — clinician roster",                              "Relationships": "Source for silver_providers"},
    {"Layer": "Bronze", "Table": "bronze_encounters",      "Source System": "EHR",                  "Key Columns": "encounter_id (TEXT), patient_id (TEXT), provider_id (TEXT), _loaded_at","Description": "Raw CSV ingestion — individual patient visits",                     "Relationships": "Source for silver_encounters"},
    {"Layer": "Bronze", "Table": "bronze_charges",         "Source System": "EHR / Charge Capture", "Key Columns": "charge_id (TEXT), encounter_id (TEXT), charge_amount (TEXT), _loaded_at","Description": "Raw CSV ingestion — line-item charges per encounter",               "Relationships": "Source for silver_charges"},
    {"Layer": "Bronze", "Table": "bronze_claims",          "Source System": "Clearinghouse",        "Key Columns": "claim_id (TEXT), encounter_id (TEXT), payer_id (TEXT), _loaded_at",   "Description": "Raw CSV ingestion — insurance claims submitted for payment",        "Relationships": "Source for silver_claims"},
    {"Layer": "Bronze", "Table": "bronze_payments",        "Source System": "Clearinghouse / ERA",  "Key Columns": "payment_id (TEXT), claim_id (TEXT), payment_amount (TEXT), _loaded_at","Description": "Raw CSV ingestion — payments received against claims",               "Relationships": "Source for silver_payments"},
    {"Layer": "Bronze", "Table": "bronze_denials",         "Source System": "Clearinghouse / ERA",  "Key Columns": "denial_id (TEXT), claim_id (TEXT), denied_amount (TEXT), _loaded_at", "Description": "Raw CSV ingestion — claim denials with reason codes",               "Relationships": "Source for silver_denials"},
    {"Layer": "Bronze", "Table": "bronze_adjustments",     "Source System": "Billing System",       "Key Columns": "adjustment_id (TEXT), claim_id (TEXT), adjustment_amount (TEXT), _loaded_at","Description": "Raw CSV ingestion — contractual write-offs and adjustments",    "Relationships": "Source for silver_adjustments"},
    {"Layer": "Bronze", "Table": "bronze_operating_costs", "Source System": "ERP / Finance",        "Key Columns": "period (TEXT), total_rcm_cost (TEXT), _loaded_at",                    "Description": "Raw CSV ingestion — monthly RCM department operating costs",       "Relationships": "Source for silver_operating_costs"},
    # ── Silver layer — derived from Bronze ETL; no external source ────────
    {"Layer": "Silver", "Table": "silver_payers",          "Source System": "—",                    "Key Columns": "payer_id PK, payer_name, payer_type, avg_reimbursement_pct REAL",     "Description": "Typed & FK-constrained — insurance payer master list",             "Relationships": "1-to-many → silver_patients, silver_claims"},
    {"Layer": "Silver", "Table": "silver_patients",        "Source System": "—",                    "Key Columns": "patient_id PK, primary_payer_id FK",                                  "Description": "Typed & FK-constrained — patient demographics and primary payer",  "Relationships": "Many-to-1 → silver_payers; 1-to-many → silver_encounters"},
    {"Layer": "Silver", "Table": "silver_providers",       "Source System": "—",                    "Key Columns": "provider_id PK, department, specialty",                               "Description": "Typed & FK-constrained — clinician roster with department",        "Relationships": "1-to-many → silver_encounters"},
    {"Layer": "Silver", "Table": "silver_encounters",      "Source System": "—",                    "Key Columns": "encounter_id PK, patient_id FK, provider_id FK, date_of_service, department, encounter_type", "Description": "Typed & FK-constrained — individual patient visits", "Relationships": "Many-to-1 → silver_patients, silver_providers; 1-to-many → silver_charges, silver_claims"},
    {"Layer": "Silver", "Table": "silver_charges",         "Source System": "—",                    "Key Columns": "charge_id PK, encounter_id FK, charge_amount REAL, units INTEGER",    "Description": "Typed & FK-constrained — line-item charges per encounter",         "Relationships": "Many-to-1 → silver_encounters"},
    {"Layer": "Silver", "Table": "silver_claims",          "Source System": "—",                    "Key Columns": "claim_id PK, encounter_id FK, patient_id FK, payer_id FK, total_charge_amount REAL, claim_status, is_clean_claim INTEGER", "Description": "Typed & FK-constrained — insurance claims; source of truth for KPIs", "Relationships": "Many-to-1 → silver_encounters, silver_payers; 1-to-many → silver_payments, silver_denials, silver_adjustments"},
    {"Layer": "Silver", "Table": "silver_payments",        "Source System": "—",                    "Key Columns": "payment_id PK, claim_id FK, payment_amount REAL, is_accurate_payment INTEGER", "Description": "Typed & FK-constrained — payments received against claims",  "Relationships": "Many-to-1 → silver_claims"},
    {"Layer": "Silver", "Table": "silver_denials",         "Source System": "—",                    "Key Columns": "denial_id PK, claim_id FK, denial_reason_code, denied_amount REAL, appeal_status, recovered_amount REAL", "Description": "Typed & FK-constrained — claim denials with reason codes and appeal tracking", "Relationships": "Many-to-1 → silver_claims"},
    {"Layer": "Silver", "Table": "silver_adjustments",     "Source System": "—",                    "Key Columns": "adjustment_id PK, claim_id FK, adjustment_type_code, adjustment_amount REAL", "Description": "Typed & FK-constrained — contractual write-offs and balance adjustments", "Relationships": "Many-to-1 → silver_claims"},
    {"Layer": "Silver", "Table": "silver_operating_costs", "Source System": "—",                    "Key Columns": "period PK, total_rcm_cost REAL",                                      "Description": "Typed & FK-constrained — monthly RCM department operating costs", "Relationships": "Standalone (joined by period/month to silver_claims)"},
    # ── Gold layer — SQL views over Silver; no external source ────────────
    {"Layer": "Gold",   "Table": "gold_monthly_kpis",          "Source System": "—", "Key Columns": "period, claim_count, total_charges, total_payments, clean_claim_rate, denial_rate, gcr", "Description": "SQL VIEW — monthly KPI aggregations across all claims",           "Relationships": "Aggregates silver_claims, silver_payments"},
    {"Layer": "Gold",   "Table": "gold_payer_performance",     "Source System": "—", "Key Columns": "payer_id, payer_name, total_claims, total_charges, total_payments, collection_rate, denial_rate", "Description": "SQL VIEW — per-payer revenue and denial metrics",       "Relationships": "Aggregates silver_claims, silver_payments, silver_payers"},
    {"Layer": "Gold",   "Table": "gold_department_performance","Source System": "—", "Key Columns": "department, encounter_count, total_charges, total_payments, collection_rate, avg_payment_per_encounter", "Description": "SQL VIEW — revenue and volume by clinical department", "Relationships": "Aggregates silver_encounters, silver_claims, silver_payments"},
    {"Layer": "Gold",   "Table": "gold_ar_aging",              "Source System": "—", "Key Columns": "aging_bucket, claim_count, total_ar, pct_of_total",               "Description": "SQL VIEW — outstanding A/R bucketed into 0-30, 31-60, 61-90, 91-120, 120+ day bands", "Relationships": "Aggregates silver_claims, silver_payments"},
    {"Layer": "Gold",   "Table": "gold_denial_analysis",       "Source System": "—", "Key Columns": "denial_reason_code, description, count, total_denied, total_recovered, recovery_rate", "Description": "SQL VIEW — denial volume and recovery rate by reason code", "Relationships": "Aggregates silver_denials"},
]

# Visual colours for CSV source system categories in the lineage diagram.
# Keyed by system NAME (from meta_kg_nodes.source_system) — the name itself
# is the single source of truth in the DB; only the colour is layout data.
# Colours align with RCM_COLORS.
_SOURCE_SYSTEM_COLORS = {
    "Payer Master":          "#0EA5E9",  # sky   — external contract data
    "EHR":                   "#10B981",  # green — clinical
    "EHR / Charge Capture":  "#10B981",  # green — clinical (CDM lives in EHR)
    "Clearinghouse":         "#1E6FBF",  # blue  — claims mgmt gateway
    "Clearinghouse / ERA":   "#1E6FBF",  # blue  — electronic remittance / payer response
    "Billing System":        "#6366F1",  # indigo — write-off posting
    "ERP / Finance":         "#14B8A6",  # teal  — GL cost centre reports
}


# ── Knowledge Graph data (module-level for AI app consumption) ────────

_KG_NODES = [
    # Reference entities (blue) — outer ring top
    {"id": "payers",    "label": "payers",    "x": 5.0, "y": 9.0, "color": "#5b8dee", "size": 30,
     "group": "Reference", "source_system": "Payer Master",
     "hover": "silver_payers: payer_id PK, payer_name, payer_type, avg_reimbursement_pct REAL"},
    {"id": "patients",  "label": "patients",  "x": 1.5, "y": 7.0, "color": "#5b8dee", "size": 30,
     "group": "Reference", "source_system": "EHR",
     "hover": "silver_patients: patient_id PK, primary_payer_id FK → silver_payers"},
    {"id": "providers", "label": "providers", "x": 8.5, "y": 7.0, "color": "#5b8dee", "size": 30,
     "group": "Reference", "source_system": "EHR",
     "hover": "silver_providers: provider_id PK, department, specialty"},
    # Central hub
    {"id": "encounters", "label": "encounters", "x": 5.0, "y": 5.5, "color": "#38c172", "size": 36,
     "group": "Transactional", "source_system": "EHR",
     "hover": "silver_encounters: encounter_id PK, patient_id FK, provider_id FK, date_of_service, department, encounter_type"},
    # Claims hub
    {"id": "claims", "label": "claims", "x": 5.0, "y": 3.0, "color": "#38c172", "size": 36,
     "group": "Transactional", "source_system": "Clearinghouse",
     "hover": "silver_claims: claim_id PK, encounter_id FK, patient_id FK, payer_id FK, date_of_service, submission_date, total_charge_amount REAL, claim_status, is_clean_claim INTEGER"},
    # Leaf transactional nodes
    {"id": "charges",     "label": "charges",     "x": 1.5, "y": 4.5, "color": "#38c172", "size": 26,
     "group": "Transactional", "source_system": "EHR / Charge Capture",
     "hover": "silver_charges: charge_id PK, encounter_id FK, charge_amount REAL, units INTEGER, service_date, post_date"},
    {"id": "payments",    "label": "payments",    "x": 2.5, "y": 1.0, "color": "#38c172", "size": 26,
     "group": "Transactional", "source_system": "Clearinghouse / ERA",
     "hover": "silver_payments: payment_id PK, claim_id FK, payment_amount REAL, is_accurate_payment INTEGER"},
    {"id": "denials",     "label": "denials",     "x": 5.0, "y": 0.5, "color": "#38c172", "size": 26,
     "group": "Transactional", "source_system": "Clearinghouse / ERA",
     "hover": "silver_denials: denial_id PK, claim_id FK, denial_reason_code, denied_amount REAL, appeal_status, recovered_amount REAL"},
    {"id": "adjustments", "label": "adjustments", "x": 7.5, "y": 1.0, "color": "#38c172", "size": 26,
     "group": "Transactional", "source_system": "Billing System",
     "hover": "silver_adjustments: adjustment_id PK, claim_id FK, adjustment_type_code, adjustment_amount REAL"},
    # Operational
    {"id": "operating_costs", "label": "operating\ncosts", "x": 9.0, "y": 4.5, "color": "#e8a838", "size": 26,
     "group": "Operational", "source_system": "ERP / Finance",
     "hover": "silver_operating_costs: period PK, total_rcm_cost REAL"},
]

_KG_EDGES = [
    {"source": "payers",    "target": "patients",      "label": "1:N (primary_payer_id)"},
    {"source": "patients",  "target": "encounters",    "label": "1:N (patient_id)"},
    {"source": "providers", "target": "encounters",    "label": "1:N (provider_id)"},
    {"source": "encounters","target": "charges",       "label": "1:N (encounter_id)"},
    {"source": "encounters","target": "claims",        "label": "1:N (encounter_id)"},
    {"source": "payers",    "target": "claims",        "label": "1:N (payer_id)"},
    {"source": "claims",    "target": "payments",      "label": "1:N (claim_id)"},
    {"source": "claims",    "target": "denials",       "label": "1:N (claim_id)"},
    {"source": "claims",    "target": "adjustments",   "label": "1:N (claim_id)"},
]

_KG_RELATIONSHIPS = [
    {"parent_table": "payers",    "child_table": "patients",     "join_column": "primary_payer_id", "cardinality": "1:N", "business_meaning": "Each patient has one primary payer"},
    {"parent_table": "payers",    "child_table": "claims",       "join_column": "payer_id",         "cardinality": "1:N", "business_meaning": "Claims are billed to one payer"},
    {"parent_table": "patients",  "child_table": "encounters",   "join_column": "patient_id",       "cardinality": "1:N", "business_meaning": "A patient can have many visits"},
    {"parent_table": "providers", "child_table": "encounters",   "join_column": "provider_id",      "cardinality": "1:N", "business_meaning": "A provider sees many patients"},
    {"parent_table": "encounters","child_table": "charges",      "join_column": "encounter_id",     "cardinality": "1:N", "business_meaning": "Each visit generates line-item charges"},
    {"parent_table": "encounters","child_table": "claims",       "join_column": "encounter_id",     "cardinality": "1:N", "business_meaning": "Each visit produces one or more insurance claims"},
    {"parent_table": "claims",    "child_table": "payments",     "join_column": "claim_id",         "cardinality": "1:N", "business_meaning": "A claim may receive partial or split payments"},
    {"parent_table": "claims",    "child_table": "denials",      "join_column": "claim_id",         "cardinality": "1:N", "business_meaning": "A claim can be denied once or multiple times"},
    {"parent_table": "claims",    "child_table": "adjustments",  "join_column": "claim_id",         "cardinality": "1:N", "business_meaning": "Contractual write-offs are applied per claim"},
]


# ── Semantic Layer data ───────────────────────────────────────────────
# NOTE: render_semantic_layer() queries meta_semantic_layer from SQLite.
# This fallback list is used only when the DB is unavailable.

_SEMANTIC_LAYER_FALLBACK = [
    {"business_concept": "Revenue",       "kpi_name": "Gross Collection Rate",       "silver_columns": "silver_claims.total_charge_amount, silver_payments.payment_amount",                                                              "formula": "SUM(payments)/SUM(charges)×100",              "business_rule": "Measures total collections vs. gross billed"},
    {"business_concept": "Revenue",       "kpi_name": "Bad Debt Rate",               "silver_columns": "silver_adjustments.adjustment_type_code, silver_adjustments.adjustment_amount, silver_claims.total_charge_amount",               "formula": "SUM(bad_debt_adj)/SUM(charges)×100",           "business_rule": "Write-offs where type_code indicates bad debt"},
    {"business_concept": "Revenue",       "kpi_name": "Avg Reimbursement/Encounter", "silver_columns": "silver_payments.payment_amount, silver_encounters.encounter_id",                                                                 "formula": "SUM(payments)/COUNT(encounters)",              "business_rule": "Revenue efficiency per patient visit"},
    {"business_concept": "Collections",   "kpi_name": "Net Collection Rate",         "silver_columns": "silver_payments.payment_amount, silver_claims.total_charge_amount, silver_adjustments.adjustment_amount",                        "formula": "Payments/(Charges−Adjustments)×100",           "business_rule": "Adjustments remove contractually non-collectible amounts"},
    {"business_concept": "Collections",   "kpi_name": "Cost to Collect",             "silver_columns": "silver_operating_costs.total_rcm_cost, silver_payments.payment_amount",                                                          "formula": "RCM Cost/Collections×100",                     "business_rule": "Billing dept efficiency; target <3%"},
    {"business_concept": "Claims Quality","kpi_name": "Clean Claim Rate",            "silver_columns": "silver_claims.is_clean_claim",                                                                                                    "formula": "SUM(is_clean_claim)/COUNT(claims)×100",        "business_rule": "Claims passing payer edits on first submission"},
    {"business_concept": "Claims Quality","kpi_name": "Denial Rate",                 "silver_columns": "silver_claims.claim_status",                                                                                                      "formula": "COUNT(status='Denied')/COUNT(claims)×100",     "business_rule": "Industry benchmark <5%"},
    {"business_concept": "Claims Quality","kpi_name": "First-Pass Rate",             "silver_columns": "silver_claims.claim_status, silver_claims.is_clean_claim",                                                                       "formula": "Resolved on first pass/Total×100",             "business_rule": "Resolved = Paid or legitimately Denied w/o rework"},
    {"business_concept": "Claims Quality","kpi_name": "Charge Lag",                  "silver_columns": "silver_claims.submission_date, silver_claims.date_of_service",                                                                   "formula": "AVG(submission_date − date_of_service)",       "business_rule": "Target <3 days; delays increase A/R balance"},
    {"business_concept": "A/R Health",    "kpi_name": "Days in A/R",                 "silver_columns": "silver_claims.total_charge_amount, silver_payments.payment_amount",                                                              "formula": "(Charges−Payments)/(Monthly Charges/30)",      "business_rule": "Target <40 days; >50 is critical"},
    {"business_concept": "A/R Health",    "kpi_name": "A/R Aging",                   "silver_columns": "silver_claims.date_of_service, silver_claims.claim_status, silver_payments.payment_amount",                                      "formula": "Outstanding bucketed by age in days",          "business_rule": "90+ day bucket should be <15% of total A/R"},
    {"business_concept": "A/R Health",    "kpi_name": "Payment Accuracy Rate",       "silver_columns": "silver_payments.is_accurate_payment",                                                                                             "formula": "SUM(is_accurate_payment)/COUNT×100",           "business_rule": "Inaccurate payments require follow-up with payer"},
    {"business_concept": "Recovery",      "kpi_name": "Appeal Success Rate",         "silver_columns": "silver_denials.appeal_status",                                                                                                    "formula": "Successful/Total Appealed×100",                "business_rule": "Target >50%; tracks ability to recover denied revenue"},
    {"business_concept": "Payer Perf.",   "kpi_name": "Payer Mix",                   "silver_columns": "silver_payments.payment_amount, silver_payers.payer_type, silver_claims.payer_id",                                              "formula": "SUM(payments) GROUP BY payer_type",            "business_rule": "High self-pay mix → higher collection risk"},
    {"business_concept": "Payer Perf.",   "kpi_name": "Denial Rate by Payer",        "silver_columns": "silver_claims.claim_status, silver_claims.payer_id, silver_payers.payer_name",                                                  "formula": "Denied/Total GROUP BY payer",                  "business_rule": "Identifies payers with problematic contracts/edits"},
    {"business_concept": "Dept Perf.",    "kpi_name": "Department Performance",      "silver_columns": "silver_encounters.department, silver_payments.payment_amount, silver_claims.encounter_id",                                       "formula": "SUM(payments), COUNT(encounters) GROUP BY dept","business_rule": "Revenue and volume by clinical department"},
]

# Aliases for database.py imports (which expect these names without _FALLBACK suffix)
_KPI_CATALOG = _KPI_CATALOG_FALLBACK
_SEMANTIC_LAYER = _SEMANTIC_LAYER_FALLBACK


# ── Page 1: Data Catalog ──────────────────────────────────────────────

def render_data_catalog():
    """Searchable KPI catalog and data tables reference — data pulled live from meta_kpi_catalog."""
    st.title("Data Catalog")
    st.caption("Reference guide for all KPI metrics and data tables. Sourced live from the meta_kpi_catalog table.")

    # ── KPI section — query meta_kpi_catalog from DB ──────────────────
    st.subheader("KPI Metrics Catalog")

    raw = _query_meta("""
        SELECT metric_name  AS "Metric",
               category     AS "Category",
               definition   AS "Definition",
               formula      AS "Formula",
               COALESCE(benchmark, '—') AS "Benchmark"
        FROM   meta_kpi_catalog
        ORDER  BY category, metric_name
    """)
    if raw.empty:
        # Fallback to static list when DB isn't ready
        raw = pd.DataFrame(_KPI_CATALOG_FALLBACK).rename(columns={
            "Metric": "Metric", "Category": "Category",
            "Definition": "Definition", "Formula": "Formula",
        })
        raw["Benchmark"] = "—"
        st.info("Live DB unavailable — showing static fallback catalog.")

    total = len(raw)
    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("Search metrics", placeholder="e.g. denial, collection, days...")
    with col2:
        categories = ["All"] + sorted(raw["Category"].dropna().unique().tolist())
        cat_filter = st.selectbox("Category", categories)

    df = raw.copy()
    if search:
        mask = (
            df["Metric"].str.contains(search, case=False, na=False)
            | df["Definition"].str.contains(search, case=False, na=False)
            | df["Formula"].str.contains(search, case=False, na=False)
        )
        df = df[mask]
    if cat_filter != "All":
        df = df[df["Category"] == cat_filter]

    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"{len(df)} of {total} metrics shown")

    st.divider()

    # ── Data tables section ──
    st.subheader("Data Tables Catalog")
    st.dataframe(pd.DataFrame(_TABLE_CATALOG), use_container_width=True, hide_index=True)


# ── Page 2: Data Lineage ──────────────────────────────────────────────

def render_data_lineage():
    """Medallion Architecture data lineage from CSV sources to the dashboard."""
    st.title("Data Lineage — Medallion Architecture")
    st.caption(
        "Three-layer medallion pipeline: "
        "**Bronze** (raw ingestion) → **Silver** (clean & typed) → "
        "**Gold** (pre-aggregated KPI views) → Dashboard."
    )

    TABLE_ORDER = [
        "payers", "patients", "providers", "encounters", "charges",
        "claims", "payments", "denials", "adjustments", "operating_costs",
    ]

    GOLD_VIEWS = {
        "gold_monthly_kpis":       ("Monthly KPIs",          ["claims", "payments"]),
        "gold_payer_performance":  ("Payer Performance",     ["payers", "claims"]),
        "gold_dept_performance":   ("Dept Performance",      ["encounters", "charges"]),
        "gold_ar_aging":           ("A/R Aging",             ["claims"]),
        "gold_denial_analysis":    ("Denial Analysis",       ["denials"]),
    }
    DASH_TABS = {
        "Executive Summary":      ["gold_monthly_kpis"],
        "Collections & Revenue":  ["gold_monthly_kpis"],
        "Claims & Denials":       ["gold_denial_analysis"],
        "A/R Aging & Cash Flow":  ["gold_ar_aging"],
        "Payer Analysis":         ["gold_payer_performance"],
        "Dept Performance":       ["gold_dept_performance"],
    }

    # Source system names from DB
    _ss_df = _query_meta(
        "SELECT entity_id, COALESCE(source_system, '') AS source_system "
        "FROM meta_kg_nodes"
    )
    _csv_source = {} if _ss_df.empty else dict(zip(_ss_df["entity_id"], _ss_df["source_system"]))

    dot = graphviz.Digraph("lineage", format="svg")
    dot.attr(rankdir="LR", bgcolor="white", fontname="Helvetica",
             nodesep="0.25", ranksep="1.2", splines="polyline")
    dot.attr("node", fontname="Helvetica", fontsize="10", style="filled",
             penwidth="1.5")
    dot.attr("edge", arrowsize="0.7", color="#888888")

    # ── CSV Source cluster ───────────────────────────────────────────
    with dot.subgraph(name="cluster_csv") as c:
        c.attr(label="CSV SOURCE FILES", style="rounded,dashed",
               color="#5b8dee", fontcolor="#1a3a8a", fontsize="11",
               bgcolor="#f0f5ff", penwidth="1.5")
        for t in TABLE_ORDER:
            sys_name = _csv_source.get(t, "")
            c.node(f"csv_{t}", label=f"{t}.csv",
                   shape="note", fillcolor="#e8f0fe", color="#5b8dee",
                   tooltip=f"Source: {sys_name}")

    # ── Bronze cluster ───────────────────────────────────────────────
    with dot.subgraph(name="cluster_bronze") as c:
        c.attr(label="BRONZE LAYER  (raw TEXT)", style="rounded",
               color="#CD7F32", fontcolor="#8B4513", fontsize="11",
               bgcolor="#fdf5ed", penwidth="1.5")
        for t in TABLE_ORDER:
            c.node(f"bronze_{t}", label=f"bronze_{t}",
                   shape="box3d", fillcolor="#f5e6d0", color="#CD7F32")

    # ── Silver cluster ───────────────────────────────────────────────
    with dot.subgraph(name="cluster_silver") as c:
        c.attr(label="SILVER LAYER  (typed + FK)", style="rounded",
               color="#606060", fontcolor="#404040", fontsize="11",
               bgcolor="#f5f5f5", penwidth="1.5")
        for t in TABLE_ORDER:
            c.node(f"silver_{t}", label=f"silver_{t}",
                   shape="box", fillcolor="#e8e8e8", color="#606060",
                   style="filled,rounded")

    # ── Gold cluster ─────────────────────────────────────────────────
    with dot.subgraph(name="cluster_gold") as c:
        c.attr(label="GOLD LAYER  (SQL views)", style="rounded",
               color="#DAA520", fontcolor="#7B5900", fontsize="11",
               bgcolor="#fffbe6", penwidth="1.5")
        for gid, (glabel, _) in GOLD_VIEWS.items():
            c.node(gid, label=glabel,
                   shape="diamond", fillcolor="#fff3c4", color="#DAA520",
                   style="filled")

    # ── Dashboard cluster ────────────────────────────────────────────
    with dot.subgraph(name="cluster_dash") as c:
        c.attr(label="DASHBOARD TABS", style="rounded",
               color="#f66d9b", fontcolor="#9b1b50", fontsize="11",
               bgcolor="#fff0f5", penwidth="1.5")
        for tab_label in DASH_TABS:
            safe_id = "tab_" + tab_label.replace(" ", "_").replace("&", "and")
            c.node(safe_id, label=tab_label,
                   shape="tab", fillcolor="#fce4ec", color="#f66d9b")

    # ── Edges: CSV → Bronze → Silver ────────────────────────────────
    for t in TABLE_ORDER:
        dot.edge(f"csv_{t}", f"bronze_{t}", color="#5b8dee88", style="dashed")
        dot.edge(f"bronze_{t}", f"silver_{t}", label="  ETL",
                 fontsize="8", fontcolor="#888888", color="#CD7F3288")

    # ── Edges: Silver → Gold ─────────────────────────────────────────
    for gid, (_, silver_sources) in GOLD_VIEWS.items():
        for src in silver_sources:
            dot.edge(f"silver_{src}", gid, color="#DAA52088")

    # ── Edges: Gold → Dashboard ──────────────────────────────────────
    for tab_label, gold_sources in DASH_TABS.items():
        safe_id = "tab_" + tab_label.replace(" ", "_").replace("&", "and")
        for gid in gold_sources:
            dot.edge(gid, safe_id, color="#f66d9b88")

    st.graphviz_chart(dot, use_container_width=True)

    # ── Source system key ──────────────────────────────────────────────
    st.subheader("Source System Key")
    st.caption("Each CSV file originates from a real-world source system. "
               "Node colours in the diagram above match the system category.")
    seen: dict = {}
    for tbl in TABLE_ORDER:
        sys_name = _csv_source.get(tbl, "Unknown")
        seen.setdefault(sys_name, []).append(f"{tbl}.csv")
    st.dataframe(
        pd.DataFrame([
            {"Source System": sys, "CSV Files": ", ".join(files)}
            for sys, files in sorted(seen.items())
        ]),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    # ── Medallion pipeline stages table ───────────────────────────────
    st.subheader("Medallion Pipeline Stages")
    pipeline_table = [
        {
            "Layer":       "Source",
            "Stage":       "1. CSV Ingest",
            "Component":   "data/*.csv (10 files)",
            "Input":       "—",
            "Output":      "Raw CSV rows",
            "Description": "Original source files; loaded once per database init.",
        },
        {
            "Layer":       "Bronze",
            "Stage":       "2. Raw Landing",
            "Component":   "database.py → load_csv_to_bronze()",
            "Input":       "CSV files",
            "Output":      "bronze_* tables (all TEXT)",
            "Description": "Data lands as-is. All columns TEXT. _loaded_at timestamp records ingestion time.",
        },
        {
            "Layer":       "Silver",
            "Stage":       "3. ETL Transform",
            "Component":   "database.py → _etl_bronze_to_silver()",
            "Input":       "bronze_* tables",
            "Output":      "silver_* tables (typed)",
            "Description": "CAST to REAL/INTEGER, normalise booleans ('True'→1), enforce FK constraints, skip NULL PKs.",
        },
        {
            "Layer":       "Gold",
            "Stage":       "4. Gold Views",
            "Component":   "database.py → gold_* SQL VIEWs (5 views)",
            "Input":       "silver_* tables",
            "Output":      "Pre-aggregated KPI views",
            "Description": "SQL VIEWs aggregate Silver by month, payer, dept, aging bucket, denial code.",
        },
        {
            "Layer":       "Silver",
            "Stage":       "5. Validate",
            "Component":   "validators.py → validate_all(db_path)",
            "Input":       "silver_* tables (via SQL)",
            "Output":      "Issue list",
            "Description": "6 SQL COUNT assertions directly against Silver tables; issues shown in sidebar Data Quality panel.",
        },
        {
            "Layer":       "Application",
            "Stage":       "6. Build FilterParams",
            "Component":   "app.py sidebar widgets",
            "Input":       "User selections (date, payer, dept, enc type)",
            "Output":      "FilterParams dataclass",
            "Description": "Packages 4 filter dimensions into a typed dataclass; passed to all 17 metric functions.",
        },
        {
            "Layer":       "Silver",
            "Stage":       "7. KPI Metrics",
            "Component":   "metrics.py → query_*(FilterParams)",
            "Input":       "FilterParams + silver_* tables (via parameterized SQL)",
            "Output":      "(scalar, DataFrame) or DataFrame per KPI",
            "Description": "17 SQL queries using build_filter_cte() CTE pattern; filters applied at the database level.",
        },
        {
            "Layer":       "Application",
            "Stage":       "8. Sidebar Widgets",
            "Component":   "data_loader.py → load_all_data()",
            "Input":       "silver_* tables",
            "Output":      "Payer / dept / enc-type dropdown options",
            "Description": "Loads minimal Silver data to populate sidebar filter dropdowns; cached by @st.cache_data.",
        },
        {
            "Layer":       "Presentation",
            "Stage":       "9. Visualize",
            "Component":   "app.py tabs 1–6",
            "Input":       "Metric results",
            "Output":      "Plotly charts, KPI scorecards",
            "Description": "6 dashboard tabs render charts and KPI cards from metric outputs.",
        },
    ]
    st.dataframe(pd.DataFrame(pipeline_table), width="stretch", hide_index=True)


# ── Page 3: Knowledge Graph ───────────────────────────────────────────

def render_knowledge_graph():
    """Interactive entity-relationship diagram — edges and descriptions sourced live from meta_kg_nodes/edges."""
    st.title("Knowledge Graph")
    st.caption("Entity relationships across the 10 data tables.")

    # ── Enrich node descriptions from meta_kg_nodes ──────────────────
    nodes_meta = _query_meta(
        "SELECT entity_id, silver_table, description FROM meta_kg_nodes"
    )
    desc_map = (
        {row.entity_id: row.description for _, row in nodes_meta.iterrows()}
        if not nodes_meta.empty else {}
    )

    # ── Build graph edges from meta_kg_edges ──────────────────────────
    edges_meta = _query_meta(
        "SELECT parent_entity, child_entity, join_column, cardinality FROM meta_kg_edges"
    )
    if not edges_meta.empty:
        live_edges = [
            {"source": row.parent_entity, "target": row.child_entity,
             "label": f"{row.cardinality}\\n({row.join_column})"}
            for _, row in edges_meta.iterrows()
        ]
    else:
        live_edges = [
            {**e, "label": e.get("label", "").replace(" (", "\\n(")}
            for e in _KG_EDGES
        ]

    st.subheader("Entity Relationship Diagram")

    # ── Category → node grouping ─────────────────────────────────────
    _GROUPS = {
        "Reference":     {"nodes": ["payers", "patients", "providers"],
                          "color": "#dbeafe", "border": "#5b8dee", "fontcolor": "#1a3a8a"},
        "Transactional": {"nodes": ["encounters", "charges", "claims",
                                     "payments", "denials", "adjustments"],
                          "color": "#dcfce7", "border": "#38c172", "fontcolor": "#166534"},
        "Operational":   {"nodes": ["operating_costs"],
                          "color": "#fef3c7", "border": "#e8a838", "fontcolor": "#92400e"},
    }

    dot = graphviz.Digraph("kg", format="svg")
    dot.attr(rankdir="TB", bgcolor="white", fontname="Helvetica",
             nodesep="0.6", ranksep="0.9", splines="ortho")
    dot.attr("node", fontname="Helvetica", fontsize="10", style="filled,rounded",
             shape="box", penwidth="1.5")
    dot.attr("edge", fontname="Helvetica", fontsize="8", color="#555555",
             arrowsize="0.8", penwidth="1.2")

    # Create subgraph clusters per category
    for group_name, cfg in _GROUPS.items():
        with dot.subgraph(name=f"cluster_{group_name}") as c:
            c.attr(label=f"  {group_name}  ", style="rounded,filled",
                   color=cfg["border"], fontcolor=cfg["fontcolor"],
                   fontsize="11", bgcolor=cfg["color"], penwidth="2")
            for nid in cfg["nodes"]:
                node_data = next((n for n in _KG_NODES if n["id"] == nid), None)
                if not node_data:
                    continue
                label = f"silver_{nid}"
                tooltip = desc_map.get(nid, node_data.get("hover", ""))
                c.node(nid, label=label, fillcolor="white",
                       color=cfg["border"], tooltip=tooltip)

    # Draw edges with cardinality labels
    for edge in live_edges:
        dot.edge(edge["source"], edge["target"],
                 label=f"  {edge['label']}  ",
                 fontcolor="#555555")

    st.graphviz_chart(dot, use_container_width=True)

    # Legend
    st.markdown("""
| Color | Category | Tables |
|-------|----------|--------|
| Blue | Reference / Master data | payers, patients, providers |
| Green | Transactional | encounters, charges, claims, payments, denials, adjustments |
| Orange | Operational | operating_costs |
""")

    # ── Relationships table — query meta_kg_edges live ─────────────────
    st.subheader("Relationships")
    rel_df = _query_meta("""
        SELECT parent_entity   AS "Parent Table",
               child_entity    AS "Child Table",
               join_column     AS "Join Column",
               cardinality     AS "Cardinality",
               business_meaning AS "Business Meaning"
        FROM   meta_kg_edges
        ORDER  BY parent_entity, child_entity
    """)
    if rel_df.empty:
        # Fallback
        rel_df = pd.DataFrame([
            {"Parent Table": r["parent_table"], "Child Table": r["child_table"],
             "Join Column": r["join_column"], "Cardinality": r["cardinality"],
             "Business Meaning": r["business_meaning"]}
            for r in _KG_RELATIONSHIPS
        ])
    st.dataframe(rel_df, use_container_width=True, hide_index=True)


# ── Page 4: Semantic Layer ────────────────────────────────────────────

def render_semantic_layer():
    """Business concept → KPI → raw column mapping."""
    st.title("Semantic Layer")
    st.caption("How business questions map to KPIs and raw data columns.")

    # ── Business Concept → KPI graph ──
    st.subheader("Business Concept Map")

    _CONCEPT_KPIS = {
        "Revenue":           {"color": "#e3342f", "bg": "#fef2f2",
                              "kpis": ["Bad Debt Rate", "Avg Reimbursement"]},
        "Collections":       {"color": "#e8a838", "bg": "#fffbeb",
                              "kpis": ["NCR", "GCR", "Cost to Collect"]},
        "Claims Quality":    {"color": "#38c172", "bg": "#f0fdf4",
                              "kpis": ["Clean Claim Rate", "Denial Rate",
                                       "First-Pass Rate", "Charge Lag"]},
        "A/R Health":        {"color": "#5b8dee", "bg": "#eff6ff",
                              "kpis": ["Days in A/R", "A/R Aging",
                                       "Payment Accuracy"]},
        "Recovery":          {"color": "#20c997", "bg": "#ecfdf5",
                              "kpis": ["Appeal Success Rate", "Denial Reasons"]},
        "Payer Perf.":       {"color": "#9561e2", "bg": "#f5f3ff",
                              "kpis": ["Payer Mix", "Denial Rate by Payer"]},
        "Dept Perf.":        {"color": "#f66d9b", "bg": "#fdf2f8",
                              "kpis": ["Dept Performance"]},
    }

    dot = graphviz.Digraph("semantic", format="svg")
    dot.attr(rankdir="TB", bgcolor="white", fontname="Helvetica",
             nodesep="0.4", ranksep="0.6", splines="polyline")
    dot.attr("node", fontname="Helvetica", fontsize="10", style="filled,rounded",
             penwidth="1.5")
    dot.attr("edge", arrowsize="0.7", penwidth="1.2")

    for concept, cfg in _CONCEPT_KPIS.items():
        # Concept node — large, colored
        cid = f"concept_{concept}"
        dot.node(cid, label=concept, shape="box",
                 fillcolor=cfg["bg"], color=cfg["color"],
                 fontcolor=cfg["color"], fontsize="12",
                 penwidth="2.5", width="1.8", height="0.5")

        # KPI nodes — smaller, white with colored border
        for kpi in cfg["kpis"]:
            kid = f"kpi_{kpi}"
            dot.node(kid, label=kpi, shape="ellipse",
                     fillcolor="white", color=cfg["color"],
                     fontsize="9")
            dot.edge(cid, kid, color=cfg["color"] + "99")

    st.subheader("Business Concepts → KPIs")
    st.graphviz_chart(dot, use_container_width=True)

    st.divider()

    # ── Semantic mapping table — query meta_semantic_layer live ────────
    st.subheader("Semantic Mapping")
    sem_df = _query_meta("""
        SELECT business_concept AS "Business Concept",
               kpi_name         AS "KPI",
               silver_columns   AS "Silver Columns",
               formula          AS "Transformation",
               business_rule    AS "Business Rule"
        FROM   meta_semantic_layer
        ORDER  BY business_concept, kpi_name
    """)
    if sem_df.empty:
        # Fallback to static list
        sem_df = pd.DataFrame([
            {"Business Concept": r["business_concept"], "KPI": r["kpi_name"],
             "Silver Columns": r["silver_columns"], "Transformation": r["formula"],
             "Business Rule": r["business_rule"]}
            for r in _SEMANTIC_LAYER_FALLBACK
        ])
    st.dataframe(sem_df, use_container_width=True, hide_index=True)

    st.divider()

    # ── Filter cascade diagram ──
    st.subheader("Sidebar Filter Cascade")
    st.markdown("""
How sidebar filter selections are applied at the database level via parameterized SQL:

```
Sidebar Selections
    start_date, end_date, payer_id, department, encounter_type
        │
        ▼
  FilterParams  (dataclass — built once per render in app.py)
        │
        ▼
  build_filter_cte()  ──▶  WITH filtered_claims AS (
                               SELECT c.*
                               FROM silver_claims c
                               LEFT JOIN silver_encounters e
                                 ON c.encounter_id = e.encounter_id
                               WHERE c.date_of_service BETWEEN ? AND ?
                                 [AND c.payer_id = ?]          -- when payer filter active
                                 [AND e.department = ?]        -- when department filter active
                                 [AND e.encounter_type = ?]    -- when enc-type filter active
                           )
        │
        ▼  (CTE reused by all 17 query_* functions)
  silver_payments    (JOIN filtered_claims ON claim_id)
  silver_denials     (JOIN filtered_claims ON claim_id)
  silver_adjustments (JOIN filtered_claims ON claim_id)
  silver_encounters  (JOIN filtered_claims ON encounter_id)
  silver_charges     (via silver_encounters ON encounter_id)

All 23 query_* functions use this CTE — filters applied at the database level, not in memory.
```
""")


# ── Page 5: AI Architecture ───────────────────────────────────────────

def render_ai_architecture():
    """Process diagram: how the AI chat tab answers RCM questions."""
    st.title("AI Assistant Architecture")
    st.caption(
        "How the AI chat tab combines the semantic layer, knowledge graph, "
        "live KPI snapshot, and a real-time SQL tool loop to answer "
        "natural-language RCM questions."
    )

    st.markdown("""
Every AI response follows a **three-stage pipeline**:

| Stage | What happens |
|-------|-------------|
| **1 · Context assembly** | A system prompt is built fresh each turn from the four `meta_*` tables (KPI definitions, semantic mappings, entity descriptions, relationships) plus the current live KPI values and active sidebar filters. |
| **2 · LLM reasoning** | The prompt + conversation history is sent to the selected model via OpenRouter.  The model decides whether to answer directly or call the `run_sql` tool. |
| **3 · Tool-calling loop** | If data is needed, the model issues a `run_sql` call with a SELECT query.  The query executes against SQLite, results are fed back, and the loop repeats until the model produces a final text answer. |
""")

    # ── Process flow diagram ──────────────────────────────────────────
    st.subheader("Process Flow Diagram")

    dot = graphviz.Digraph("ai_arch", format="svg")
    dot.attr(rankdir="TB", bgcolor="white", fontname="Helvetica",
             nodesep="0.5", ranksep="0.7", splines="ortho", compound="true")
    dot.attr("node", fontname="Helvetica", fontsize="10", style="filled,rounded",
             penwidth="1.5")
    dot.attr("edge", fontname="Helvetica", fontsize="8", penwidth="1.2",
             arrowsize="0.8")

    # ── Context Assembly cluster ─────────────────────────────────────
    with dot.subgraph(name="cluster_context") as c:
        c.attr(label="CONTEXT ASSEMBLY", style="rounded,filled",
               color="#5b8dee", fontcolor="#1a3a8a", fontsize="12",
               bgcolor="#eff6ff", penwidth="2")
        c.node("meta_kpi", "meta_kpi_catalog\n(23 KPI definitions)",
               shape="cylinder", fillcolor="#ede9fe", color="#9561e2")
        c.node("meta_sem", "meta_semantic_layer\n(concept → KPI maps)",
               shape="cylinder", fillcolor="#ede9fe", color="#9561e2")
        c.node("meta_kg", "meta_kg_nodes + edges\n(10 entities, 9 FKs)",
               shape="cylinder", fillcolor="#ede9fe", color="#9561e2")
        c.node("live_kpis", "Live KPI Snapshot\n(current filter values)",
               shape="box", fillcolor="#fef3c7", color="#e8a838")
        c.node("sys_prompt", "System Prompt",
               shape="box", fillcolor="#dbeafe", color="#5b6af0",
               fontsize="12", penwidth="2.5")

    # ── LLM Engine cluster ───────────────────────────────────────────
    with dot.subgraph(name="cluster_llm") as c:
        c.attr(label="LLM ENGINE", style="rounded,filled",
               color="#38c172", fontcolor="#166534", fontsize="12",
               bgcolor="#f0fdf4", penwidth="2")
        c.node("user_q", "User Question",
               shape="parallelogram", fillcolor="#dbeafe", color="#5b8dee")
        c.node("llm", "OpenRouter LLM\n(model selection)",
               shape="hexagon", fillcolor="#bbf7d0", color="#38c172",
               fontsize="12", penwidth="2.5", width="2")
        c.node("final_ans", "Final Answer",
               shape="parallelogram", fillcolor="#dbeafe", color="#5b8dee")

    # ── SQL Tool Loop cluster ────────────────────────────────────────
    with dot.subgraph(name="cluster_tool") as c:
        c.attr(label="SQL TOOL LOOP  (up to 8 iterations)", style="rounded,filled",
               color="#f56b00", fontcolor="#7a3000", fontsize="12",
               bgcolor="#fff7ed", penwidth="2")
        c.node("run_sql", "run_sql() Tool\n(read-only SELECT/WITH)",
               shape="box", fillcolor="#fed7aa", color="#f56b00",
               penwidth="2.5")
        c.node("silver_db", "silver_* Tables\n(10 typed tables)",
               shape="cylinder", fillcolor="#e8e8e8", color="#7a7a7a")
        c.node("gold_db", "gold_* Views\n(5 aggregated views)",
               shape="cylinder", fillcolor="#fff3c4", color="#B8860B")
        c.node("meta_db", "meta_* Tables\n(4 AI-queryable tables)",
               shape="cylinder", fillcolor="#ede9fe", color="#9561e2")

    # ── Edges: Context assembly ──────────────────────────────────────
    dot.edge("meta_kpi", "sys_prompt", label="KPI defs", color="#9561e2")
    dot.edge("meta_sem", "sys_prompt", label="mappings", color="#9561e2")
    dot.edge("meta_kg", "sys_prompt", label="schema", color="#9561e2")
    dot.edge("live_kpis", "sys_prompt", label="snapshot", color="#e8a838")

    # ── Edges: LLM flow ──────────────────────────────────────────────
    dot.edge("sys_prompt", "llm", label="context", color="#5b6af0")
    dot.edge("user_q", "llm", label="question", color="#5b8dee")
    dot.edge("llm", "final_ans", label="response", color="#38c172")

    # ── Edges: Tool calling loop ─────────────────────────────────────
    dot.edge("llm", "run_sql", label="tool call →", color="#38c172")
    dot.edge("run_sql", "llm", label="← results", color="#f56b00",
             style="dashed")

    # ── Edges: Database access ───────────────────────────────────────
    dot.edge("run_sql", "silver_db", label="SELECT", color="#7a7a7a")
    dot.edge("run_sql", "gold_db", label="SELECT", color="#B8860B")
    dot.edge("meta_db", "sys_prompt", label="build_system_prompt()",
             color="#9561e2", style="dashed")

    st.graphviz_chart(dot, use_container_width=True)

    # ── Step-by-step breakdown ────────────────────────────────────────
    st.subheader("Step-by-Step Breakdown")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Stage 1 — Context Assembly")
        st.markdown("""
`build_system_prompt()` in `src/ai_chat.py` queries four meta tables on every turn:

| Table | Content passed to LLM |
|-------|-----------------------|
| `meta_kpi_catalog` | 23 KPI names, formulas, categories, benchmarks |
| `meta_semantic_layer` | Business concept → KPI → source column mappings |
| `meta_kg_nodes` | 10 Silver-layer entity descriptions |
| `meta_kg_edges` | 9 foreign-key relationships (join paths) |

The **live KPI snapshot** appends current metric values and active sidebar
filter state so the model can answer "what is our denial rate right now?" without
needing to query the database.
""")

    with col2:
        st.markdown("#### Stage 2 — LLM Reasoning")
        st.markdown("""
The assembled system prompt, full conversation history, and the `run_sql` tool
schema are sent to **OpenRouter** in a single API call.

The model chooses one of two paths:
- **Answer directly** — if the KPI snapshot already contains the answer
- **Call `run_sql`** — if a breakdown, trend, or specific record is needed

The tool-calling loop runs up to **8 iterations** per turn, allowing multi-step
queries (e.g. fetch payer list → then query denial rates per payer).
""")

    st.markdown("#### Stage 3 — SQL Tool Loop")
    col3, col4 = st.columns(2)

    with col3:
        st.markdown("""
**`execute_sql_tool()`** in `src/ai_chat.py`:

- Accepts the query string from the model's tool call
- Validates that it is a `SELECT` or `WITH` (CTE) statement — all other
  statement types return an error without touching the database
- Executes against the local SQLite database
- Caps results at **100 rows** to stay within LLM context limits
- Returns structured `{columns, rows, row_count, total_rows, truncated}`

The formatted results are appended to the conversation as a `role: tool`
message, giving the model full visibility into what the query returned.
""")

    with col4:
        st.markdown("""
**What gets queried:**

| Target | When used |
|--------|-----------|
| `silver_*` tables | Row-level lookups, custom joins, breakdowns not in Gold views |
| `gold_*` views | Pre-aggregated KPIs — faster for summary questions |
| `meta_*` tables | Queried at prompt-build time only (not via the tool) |

**Query results in the UI:**

Each `run_sql` call appears as a collapsible expander in the chat showing
the exact SQL and a scrollable results table. Previous turns' queries are
preserved in session state and re-rendered on page reload.
""")

    # ── Session state diagram ─────────────────────────────────────────
    st.subheader("Session State & History Management")
    st.markdown("""
Two parallel stores keep the AI tab stateful across Streamlit reruns:

```
st.session_state["ai_display_turns"]          st.session_state["ai_api_messages"]
─────────────────────────────────────         ──────────────────────────────────────
For rendering the chat UI                     For sending to the OpenRouter API

[                                             [
  {role: "user",    content: "..."},            {role: "user",    content: "..."},
  {role: "assistant",                           {role: "assistant", content: "",
   content: "Final answer text",                 tool_calls: [{id, fn, args}]},
   queries: [                                  {role: "tool",
     {description, sql,                         tool_call_id: "...",
      columns, rows, truncated}                 content: "col1,col2\\nv1,v2\\n..."},
   ]},                                         {role: "assistant",
  ...                                           content: "Final answer text"},
]                                             ]
                                              (system prompt prepended fresh each turn)
```

The system prompt is **rebuilt on every turn** so new dashboard filter
selections are automatically reflected in the AI's context.
""")
