"""
Metadata Pages for Healthcare RCM Analytics Dashboard
======================================================

This module contains four supplemental pages accessible from the sidebar:
  - Data Catalog      : Searchable table of all 17 KPIs + 10 data tables
  - Data Lineage      : DAG showing the full pipeline from CSV to dashboard
  - Knowledge Graph   : Entity-relationship diagram of the 10 data entities
  - Semantic Layer    : Business concept → KPI → raw column mapping

Each render_*() function is called from app.py based on st.session_state["active_page"].
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# ── Shared graph helper ───────────────────────────────────────────────


def _draw_network_graph(nodes, edges, title, height=600):
    """
    Render a network/DAG diagram using Plotly scatter traces.

    Args:
        nodes: list of dicts with keys: id, label, x, y, color, size, hover
        edges: list of dicts with keys: source, target, label (optional)
        title: chart title string
        height: pixel height of the figure
    """
    node_map = {n["id"]: n for n in nodes}

    fig = go.Figure()

    # Draw edges first (so they appear behind nodes)
    for edge in edges:
        src = node_map[edge["source"]]
        tgt = node_map[edge["target"]]
        fig.add_trace(go.Scatter(
            x=[src["x"], tgt["x"], None],
            y=[src["y"], tgt["y"], None],
            mode="lines",
            line=dict(width=1.5, color="#aaaaaa"),
            hoverinfo="none",
            showlegend=False,
        ))
        # Edge label midpoint
        if edge.get("label"):
            mx = (src["x"] + tgt["x"]) / 2
            my = (src["y"] + tgt["y"]) / 2
            fig.add_annotation(
                x=mx, y=my,
                text=edge["label"],
                showarrow=False,
                font=dict(size=9, color="#666666"),
                bgcolor="rgba(255,255,255,0.7)",
            )

    # Draw nodes grouped by color for legend
    color_groups = {}
    for n in nodes:
        color_groups.setdefault(n.get("group", ""), []).append(n)

    for group, group_nodes in color_groups.items():
        fig.add_trace(go.Scatter(
            x=[n["x"] for n in group_nodes],
            y=[n["y"] for n in group_nodes],
            mode="markers+text",
            marker=dict(
                size=[n.get("size", 30) for n in group_nodes],
                color=[n["color"] for n in group_nodes],
                line=dict(width=1, color="white"),
            ),
            text=[n["label"] for n in group_nodes],
            textposition="bottom center",
            textfont=dict(size=10),
            hovertext=[n.get("hover", n["label"]) for n in group_nodes],
            hoverinfo="text",
            name=group if group else "Nodes",
            showlegend=bool(group),
        ))

    fig.update_layout(
        title=title,
        height=height,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(fig, theme="streamlit", width="stretch")


# ── KPI catalog data ──────────────────────────────────────────────────

_KPI_CATALOG = [
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
    {"Layer": "Bronze", "Table": "bronze_payers",          "Key Columns": "payer_id (TEXT), _loaded_at",                                       "Description": "Raw CSV ingestion — insurance payer master list",                    "Relationships": "Source for silver_payers"},
    {"Layer": "Bronze", "Table": "bronze_patients",        "Key Columns": "patient_id (TEXT), primary_payer_id (TEXT), _loaded_at",              "Description": "Raw CSV ingestion — patient demographics",                          "Relationships": "Source for silver_patients"},
    {"Layer": "Bronze", "Table": "bronze_providers",       "Key Columns": "provider_id (TEXT), department (TEXT), _loaded_at",                   "Description": "Raw CSV ingestion — clinician roster",                              "Relationships": "Source for silver_providers"},
    {"Layer": "Bronze", "Table": "bronze_encounters",      "Key Columns": "encounter_id (TEXT), patient_id (TEXT), provider_id (TEXT), _loaded_at","Description": "Raw CSV ingestion — individual patient visits",                     "Relationships": "Source for silver_encounters"},
    {"Layer": "Bronze", "Table": "bronze_charges",         "Key Columns": "charge_id (TEXT), encounter_id (TEXT), charge_amount (TEXT), _loaded_at","Description": "Raw CSV ingestion — line-item charges per encounter",               "Relationships": "Source for silver_charges"},
    {"Layer": "Bronze", "Table": "bronze_claims",          "Key Columns": "claim_id (TEXT), encounter_id (TEXT), payer_id (TEXT), _loaded_at",   "Description": "Raw CSV ingestion — insurance claims submitted for payment",        "Relationships": "Source for silver_claims"},
    {"Layer": "Bronze", "Table": "bronze_payments",        "Key Columns": "payment_id (TEXT), claim_id (TEXT), payment_amount (TEXT), _loaded_at","Description": "Raw CSV ingestion — payments received against claims",               "Relationships": "Source for silver_payments"},
    {"Layer": "Bronze", "Table": "bronze_denials",         "Key Columns": "denial_id (TEXT), claim_id (TEXT), denied_amount (TEXT), _loaded_at", "Description": "Raw CSV ingestion — claim denials with reason codes",               "Relationships": "Source for silver_denials"},
    {"Layer": "Bronze", "Table": "bronze_adjustments",     "Key Columns": "adjustment_id (TEXT), claim_id (TEXT), adjustment_amount (TEXT), _loaded_at","Description": "Raw CSV ingestion — contractual write-offs and adjustments",    "Relationships": "Source for silver_adjustments"},
    {"Layer": "Bronze", "Table": "bronze_operating_costs", "Key Columns": "period (TEXT), total_rcm_cost (TEXT), _loaded_at",                    "Description": "Raw CSV ingestion — monthly RCM department operating costs",       "Relationships": "Source for silver_operating_costs"},
    # ── Silver layer ──────────────────────────────────────────────────────
    {"Layer": "Silver", "Table": "silver_payers",          "Key Columns": "payer_id PK, payer_name, payer_type, avg_reimbursement_pct REAL",     "Description": "Typed & FK-constrained — insurance payer master list",             "Relationships": "1-to-many → silver_patients, silver_claims"},
    {"Layer": "Silver", "Table": "silver_patients",        "Key Columns": "patient_id PK, primary_payer_id FK",                                  "Description": "Typed & FK-constrained — patient demographics and primary payer",  "Relationships": "Many-to-1 → silver_payers; 1-to-many → silver_encounters"},
    {"Layer": "Silver", "Table": "silver_providers",       "Key Columns": "provider_id PK, department, specialty",                               "Description": "Typed & FK-constrained — clinician roster with department",        "Relationships": "1-to-many → silver_encounters"},
    {"Layer": "Silver", "Table": "silver_encounters",      "Key Columns": "encounter_id PK, patient_id FK, provider_id FK, date_of_service, department, encounter_type", "Description": "Typed & FK-constrained — individual patient visits", "Relationships": "Many-to-1 → silver_patients, silver_providers; 1-to-many → silver_charges, silver_claims"},
    {"Layer": "Silver", "Table": "silver_charges",         "Key Columns": "charge_id PK, encounter_id FK, charge_amount REAL, units INTEGER",    "Description": "Typed & FK-constrained — line-item charges per encounter",         "Relationships": "Many-to-1 → silver_encounters"},
    {"Layer": "Silver", "Table": "silver_claims",          "Key Columns": "claim_id PK, encounter_id FK, patient_id FK, payer_id FK, total_charge_amount REAL, claim_status, is_clean_claim INTEGER", "Description": "Typed & FK-constrained — insurance claims; source of truth for KPIs", "Relationships": "Many-to-1 → silver_encounters, silver_payers; 1-to-many → silver_payments, silver_denials, silver_adjustments"},
    {"Layer": "Silver", "Table": "silver_payments",        "Key Columns": "payment_id PK, claim_id FK, payment_amount REAL, is_accurate_payment INTEGER", "Description": "Typed & FK-constrained — payments received against claims",  "Relationships": "Many-to-1 → silver_claims"},
    {"Layer": "Silver", "Table": "silver_denials",         "Key Columns": "denial_id PK, claim_id FK, denial_reason_code, denied_amount REAL, appeal_status, recovered_amount REAL", "Description": "Typed & FK-constrained — claim denials with reason codes and appeal tracking", "Relationships": "Many-to-1 → silver_claims"},
    {"Layer": "Silver", "Table": "silver_adjustments",     "Key Columns": "adjustment_id PK, claim_id FK, adjustment_type_code, adjustment_amount REAL", "Description": "Typed & FK-constrained — contractual write-offs and balance adjustments", "Relationships": "Many-to-1 → silver_claims"},
    {"Layer": "Silver", "Table": "silver_operating_costs", "Key Columns": "period PK, total_rcm_cost REAL",                                      "Description": "Typed & FK-constrained — monthly RCM department operating costs", "Relationships": "Standalone (joined by period/month to silver_claims)"},
    # ── Gold layer ────────────────────────────────────────────────────────
    {"Layer": "Gold",   "Table": "gold_monthly_kpis",          "Key Columns": "period, claim_count, total_charges, total_payments, clean_claim_rate, denial_rate, gcr", "Description": "SQL VIEW — monthly KPI aggregations across all claims",           "Relationships": "Aggregates silver_claims, silver_payments"},
    {"Layer": "Gold",   "Table": "gold_payer_performance",     "Key Columns": "payer_id, payer_name, total_claims, total_charges, total_payments, collection_rate, denial_rate", "Description": "SQL VIEW — per-payer revenue and denial metrics",       "Relationships": "Aggregates silver_claims, silver_payments, silver_payers"},
    {"Layer": "Gold",   "Table": "gold_department_performance","Key Columns": "department, encounter_count, total_charges, total_payments, collection_rate, avg_payment_per_encounter", "Description": "SQL VIEW — revenue and volume by clinical department", "Relationships": "Aggregates silver_encounters, silver_claims, silver_payments"},
    {"Layer": "Gold",   "Table": "gold_ar_aging",              "Key Columns": "aging_bucket, claim_count, total_ar, pct_of_total",               "Description": "SQL VIEW — outstanding A/R bucketed into 0-30, 31-60, 61-90, 91-120, 120+ day bands", "Relationships": "Aggregates silver_claims, silver_payments"},
    {"Layer": "Gold",   "Table": "gold_denial_analysis",       "Key Columns": "denial_reason_code, description, count, total_denied, total_recovered, recovery_rate", "Description": "SQL VIEW — denial volume and recovery rate by reason code", "Relationships": "Aggregates silver_denials"},
]


# ── Knowledge Graph data (module-level for AI app consumption) ────────

_KG_NODES = [
    # Reference entities (blue) — outer ring top
    {"id": "payers",    "label": "payers",    "x": 5.0, "y": 9.0, "color": "#5b8dee", "size": 30,
     "group": "Reference", "hover": "silver_payers: payer_id PK, payer_name, payer_type, avg_reimbursement_pct REAL"},
    {"id": "patients",  "label": "patients",  "x": 1.5, "y": 7.0, "color": "#5b8dee", "size": 30,
     "group": "Reference", "hover": "silver_patients: patient_id PK, primary_payer_id FK → silver_payers"},
    {"id": "providers", "label": "providers", "x": 8.5, "y": 7.0, "color": "#5b8dee", "size": 30,
     "group": "Reference", "hover": "silver_providers: provider_id PK, department, specialty"},
    # Central hub
    {"id": "encounters", "label": "encounters", "x": 5.0, "y": 5.5, "color": "#38c172", "size": 36,
     "group": "Transactional", "hover": "silver_encounters: encounter_id PK, patient_id FK, provider_id FK, date_of_service, department, encounter_type"},
    # Claims hub
    {"id": "claims", "label": "claims", "x": 5.0, "y": 3.0, "color": "#38c172", "size": 36,
     "group": "Transactional", "hover": "silver_claims: claim_id PK, encounter_id FK, patient_id FK, payer_id FK, date_of_service, submission_date, total_charge_amount REAL, claim_status, is_clean_claim INTEGER"},
    # Leaf transactional nodes
    {"id": "charges",     "label": "charges",     "x": 1.5, "y": 4.5, "color": "#38c172", "size": 26,
     "group": "Transactional", "hover": "silver_charges: charge_id PK, encounter_id FK, charge_amount REAL, units INTEGER, service_date, post_date"},
    {"id": "payments",    "label": "payments",    "x": 2.5, "y": 1.0, "color": "#38c172", "size": 26,
     "group": "Transactional", "hover": "silver_payments: payment_id PK, claim_id FK, payment_amount REAL, is_accurate_payment INTEGER"},
    {"id": "denials",     "label": "denials",     "x": 5.0, "y": 0.5, "color": "#38c172", "size": 26,
     "group": "Transactional", "hover": "silver_denials: denial_id PK, claim_id FK, denial_reason_code, denied_amount REAL, appeal_status, recovered_amount REAL"},
    {"id": "adjustments", "label": "adjustments", "x": 7.5, "y": 1.0, "color": "#38c172", "size": 26,
     "group": "Transactional", "hover": "silver_adjustments: adjustment_id PK, claim_id FK, adjustment_type_code, adjustment_amount REAL"},
    # Operational
    {"id": "operating_costs", "label": "operating\ncosts", "x": 9.0, "y": 4.5, "color": "#e8a838", "size": 26,
     "group": "Operational", "hover": "silver_operating_costs: period PK, total_rcm_cost REAL"},
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


# ── Semantic Layer data (module-level for AI app consumption) ─────────

_SEMANTIC_LAYER = [
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


# ── Page 1: Data Catalog ──────────────────────────────────────────────

def render_data_catalog():
    """Searchable table of all 17 KPIs and 10 data tables."""
    st.title("Data Catalog")
    st.caption("Reference guide for all metrics and data tables used in this dashboard.")

    # ── KPI section ──
    st.subheader("KPI Metrics Catalog")
    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("Search metrics", placeholder="e.g. denial, collection, days...")
    with col2:
        categories = ["All"] + sorted({r["Category"] for r in _KPI_CATALOG})
        cat_filter = st.selectbox("Category", categories)

    df = pd.DataFrame(_KPI_CATALOG)
    if search:
        mask = (
            df["Metric"].str.contains(search, case=False, na=False) |
            df["Definition"].str.contains(search, case=False, na=False) |
            df["Formula"].str.contains(search, case=False, na=False)
        )
        df = df[mask]
    if cat_filter != "All":
        df = df[df["Category"] == cat_filter]

    st.dataframe(df, width="stretch", hide_index=True)
    st.caption(f"{len(df)} of {len(_KPI_CATALOG)} metrics shown")

    st.divider()

    # ── Data tables section ──
    st.subheader("Data Tables Catalog")
    st.dataframe(pd.DataFrame(_TABLE_CATALOG), width="stretch", hide_index=True)


# ── Page 2: Data Lineage ──────────────────────────────────────────────

def render_data_lineage():
    """Medallion Architecture data lineage from CSV sources to the dashboard."""
    st.title("Data Lineage — Medallion Architecture")
    st.caption(
        "Three-layer medallion pipeline: "
        "**Bronze** (raw ingestion) → **Silver** (clean & typed) → "
        "**Gold** (pre-aggregated KPI views) → Dashboard."
    )

    # ── Layout constants ───────────────────────────────────────────────
    TABLE_ORDER = [
        "payers", "patients", "providers", "encounters", "charges",
        "claims", "payments", "denials", "adjustments", "operating_costs",
    ]
    Y_STEP = 1.25
    tbl_y = {t: i * Y_STEP for i, t in enumerate(TABLE_ORDER)}

    X_CSV, X_BRONZE, X_SILVER, X_GOLD, X_DASH = 0.7, 2.8, 5.2, 7.5, 9.5
    Y_TOP = tbl_y["operating_costs"] + Y_STEP * 0.8

    GOLD_VIEWS = [
        ("gold_monthly_kpis",    "gold_monthly_kpis",       tbl_y["patients"],           "Monthly KPIs: GCR, denial rate, clean claim rate by month"),
        ("gold_payer_perf",      "gold_payer_performance",  tbl_y["encounters"],          "Per-payer: denial rate, collection rate, total revenue"),
        ("gold_dept_perf",       "gold_dept_performance",   tbl_y["claims"],              "Per-dept: encounter count, charges, payments, rev/encounter"),
        ("gold_ar_aging",        "gold_ar_aging",           tbl_y["denials"],             "Open claims bucketed: 0-30, 31-60, 61-90, 91-120, 120+ days"),
        ("gold_denial_analysis", "gold_denial_analysis",    tbl_y["operating_costs"],    "Denial codes: count, denied $, recovered $, appeal win rate"),
    ]
    DASH_TABS = [
        ("tab_exec",   "Executive\nSummary",      tbl_y["payers"]),
        ("tab_rev",    "Collections &\nRevenue",  tbl_y["providers"]),
        ("tab_claims", "Claims &\nDenials",       tbl_y["charges"]),
        ("tab_ar",     "A/R Aging &\nCash Flow",  tbl_y["payments"]),
        ("tab_payer",  "Payer\nAnalysis",          tbl_y["adjustments"]),
        ("tab_dept",   "Department\nPerf.",        tbl_y["operating_costs"]),
    ]

    # ── Accumulate nodes and edges ─────────────────────────────────────
    nodes = []
    edges = []
    npos  = {}  # node_id → (x, y) for edge drawing

    def _n(nid, label, x, y, color, size, group, hover):
        nodes.append(dict(id=nid, label=label, x=x, y=y,
                          color=color, size=size, group=group, hover=hover))
        npos[nid] = (x, y)

    def _e(src, tgt):
        edges.append((src, tgt))

    # CSV source nodes
    for t in TABLE_ORDER:
        _n(f"csv_{t}", f"{t}.csv", X_CSV, tbl_y[t],
           "#5b8dee", 11, "CSV Source", f"Source file: data/{t}.csv")

    # Bronze table nodes
    for t in TABLE_ORDER:
        _n(f"bronze_{t}", f"bronze_{t}", X_BRONZE, tbl_y[t],
           "#CD7F32", 14, "Bronze Table",
           f"bronze_{t}: all TEXT columns + _loaded_at timestamp")

    # Silver table nodes
    for t in TABLE_ORDER:
        _n(f"silver_{t}", f"silver_{t}", X_SILVER, tbl_y[t],
           "#7a7a7a", 14, "Silver Table",
           f"silver_{t}: typed columns (REAL/INTEGER), FK constraints enforced")

    # Gold view nodes
    for gid, glabel, gy, ghover in GOLD_VIEWS:
        _n(gid, glabel, X_GOLD, gy, "#DAA520", 20, "Gold View", ghover)

    # Dashboard tab nodes
    for tid, tlabel, ty in DASH_TABS:
        _n(tid, tlabel, X_DASH, ty, "#f66d9b", 17, "Dashboard Tab",
           f"Dashboard tab: {tlabel.replace(chr(10), ' ')}")

    # CSV → Bronze edges (raw ingestion)
    for t in TABLE_ORDER:
        _e(f"csv_{t}", f"bronze_{t}")

    # Bronze → Silver edges (ETL: type casting & validation)
    for t in TABLE_ORDER:
        _e(f"bronze_{t}", f"silver_{t}")

    # Silver → Gold edges (SQL aggregation, representative joins)
    for src, tgt in [
        ("silver_claims",     "gold_monthly_kpis"),
        ("silver_payments",   "gold_monthly_kpis"),
        ("silver_payers",     "gold_payer_perf"),
        ("silver_claims",     "gold_payer_perf"),
        ("silver_encounters", "gold_dept_perf"),
        ("silver_charges",    "gold_dept_perf"),
        ("silver_claims",     "gold_ar_aging"),
        ("silver_denials",    "gold_denial_analysis"),
    ]:
        _e(src, tgt)

    # Gold → Dashboard edges
    for src, tgt in [
        ("gold_monthly_kpis",    "tab_exec"),
        ("gold_monthly_kpis",    "tab_rev"),
        ("gold_payer_perf",      "tab_payer"),
        ("gold_dept_perf",       "tab_dept"),
        ("gold_ar_aging",        "tab_ar"),
        ("gold_denial_analysis", "tab_claims"),
    ]:
        _e(src, tgt)

    # ── Build Plotly figure ────────────────────────────────────────────
    fig = go.Figure()

    # Zone background rectangles
    ZONE_Y0, ZONE_Y1 = -0.8, Y_TOP + 0.2
    zone_defs = [
        # x0,  x1,  fill,                          border,                        label,          tx,   color
        (-0.3,  4.0, "rgba(205,127,50,0.07)",  "rgba(205,127,50,0.30)",  "BRONZE LAYER",  1.85, "#8B4513"),
        ( 4.0,  6.8, "rgba(150,150,150,0.07)", "rgba(150,150,150,0.30)", "SILVER LAYER",  5.40, "#505050"),
        ( 6.8, 10.8, "rgba(255,215,  0,0.07)", "rgba(218,165, 32,0.30)", "GOLD LAYER",    8.80, "#7B5900"),
    ]
    for x0, x1, fill, border, zlabel, tx, tc in zone_defs:
        fig.add_shape(
            type="rect", x0=x0, y0=ZONE_Y0, x1=x1, y1=ZONE_Y1,
            fillcolor=fill, line=dict(color=border, width=1.5), layer="below",
        )
        fig.add_annotation(
            x=tx, y=Y_TOP + 0.1, text=f"<b>{zlabel}</b>",
            showarrow=False, font=dict(size=12, color=tc), xanchor="center",
        )

    # ETL step label
    fig.add_annotation(
        x=(X_BRONZE + X_SILVER) / 2, y=Y_TOP - 0.1,
        text="<i>ETL →</i>",
        showarrow=False, font=dict(size=9, color="#888"),
        bgcolor="rgba(255,255,255,0.8)",
    )

    # Draw edges
    for src_id, tgt_id in edges:
        sx, sy = npos[src_id]
        tx, ty = npos[tgt_id]
        fig.add_trace(go.Scatter(
            x=[sx, tx, None], y=[sy, ty, None],
            mode="lines",
            line=dict(width=0.8, color="#c8c8c8"),
            hoverinfo="none",
            showlegend=False,
        ))

    # Draw nodes, grouped by layer for the legend
    color_groups: dict = {}
    for n in nodes:
        color_groups.setdefault(n["group"], []).append(n)

    for group, gnodes in color_groups.items():
        fig.add_trace(go.Scatter(
            x=[n["x"] for n in gnodes],
            y=[n["y"] for n in gnodes],
            mode="markers+text",
            marker=dict(
                size=[n["size"] for n in gnodes],
                color=[n["color"] for n in gnodes],
                line=dict(width=1, color="white"),
            ),
            text=[n["label"] for n in gnodes],
            textposition="bottom center",
            textfont=dict(size=7),
            hovertext=[n.get("hover", n["label"]) for n in gnodes],
            hoverinfo="text",
            name=group,
            showlegend=True,
        ))

    fig.update_layout(
        title="Data Pipeline — Medallion Architecture (Bronze → Silver → Gold)",
        height=800,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.6, 11.2]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-1.3, Y_TOP + 0.8]),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=20, r=20, t=55, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(fig, theme="streamlit", width="stretch")

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
    """Interactive entity-relationship diagram of the 10 data entities."""
    st.title("Knowledge Graph")
    st.caption("Entity relationships across the 10 data tables. Hover nodes for column details.")

    _draw_network_graph(_KG_NODES, _KG_EDGES, "Entity Relationship Diagram", height=620)

    # Legend
    st.markdown("""
| Color | Category | Tables |
|-------|----------|--------|
| 🔵 Blue | Reference / Master data | payers, patients, providers |
| 🟢 Green | Transactional | encounters, charges, claims, payments, denials, adjustments |
| 🟠 Orange | Operational | operating_costs |
""")

    st.subheader("Relationships")
    rel_display = [
        {
            "Parent Table": r["parent_table"],
            "Child Table":  r["child_table"],
            "Join Column":  r["join_column"],
            "Cardinality":  r["cardinality"],
            "Business Meaning": r["business_meaning"],
        }
        for r in _KG_RELATIONSHIPS
    ]
    st.dataframe(pd.DataFrame(rel_display), width="stretch", hide_index=True)


# ── Page 4: Semantic Layer ────────────────────────────────────────────

def render_semantic_layer():
    """Business concept → KPI → raw column mapping."""
    st.title("Semantic Layer")
    st.caption("How business questions map to KPIs and raw data columns.")

    # ── Business Concept → KPI graph ──
    st.subheader("Business Concept Map")

    concepts = [
        ("Revenue",              4.5, 8.0, "#e3342f"),
        ("Collections",          1.5, 6.0, "#e8a838"),
        ("Claims Quality",       4.5, 6.0, "#38c172"),
        ("A/R Health",           7.5, 6.0, "#5b8dee"),
        ("Payer Performance",    2.0, 3.5, "#9561e2"),
        ("Dept Performance",     7.0, 3.5, "#f66d9b"),
        ("Recovery & Appeals",   4.5, 3.5, "#20c997"),
    ]
    kpis = [
        ("NCR",           1.0, 4.5, "#e8a838", "Collections"),
        ("GCR",           2.0, 4.5, "#e8a838", "Collections"),
        ("Cost-to-Collect",3.0, 4.5, "#e8a838", "Collections"),
        ("DAR",           6.5, 4.5, "#5b8dee", "A/R Health"),
        ("A/R Aging",     7.5, 4.5, "#5b8dee", "A/R Health"),
        ("Pmt Accuracy",  8.5, 4.5, "#5b8dee", "A/R Health"),
        ("Clean Claim",   3.5, 4.5, "#38c172", "Claims Quality"),
        ("Denial Rate",   4.5, 4.5, "#38c172", "Claims Quality"),
        ("First-Pass",    5.5, 4.5, "#38c172", "Claims Quality"),
        ("Charge Lag",    4.5, 2.5, "#38c172", "Claims Quality"),
        ("Bad Debt",      4.5, 7.0, "#e3342f", "Revenue"),
        ("Avg Reimb",     5.5, 7.0, "#e3342f", "Revenue"),
        ("Appeal Success",3.5, 2.5, "#20c997", "Recovery & Appeals"),
        ("Denial Reasons",5.5, 2.5, "#20c997", "Recovery & Appeals"),
        ("Payer Mix",     1.5, 2.0, "#9561e2", "Payer Performance"),
        ("Denial/Payer",  2.5, 2.0, "#9561e2", "Payer Performance"),
        ("Dept Perf.",    7.0, 2.0, "#f66d9b", "Dept Performance"),
    ]

    nodes = []
    for name, x, y, color in concepts:
        nodes.append({"id": name, "label": name, "x": x, "y": y, "color": color,
                      "size": 34, "group": "Business Concept", "hover": f"Business Concept: {name}"})
    for name, x, y, color, parent in kpis:
        nodes.append({"id": name, "label": name, "x": x, "y": y, "color": color,
                      "size": 20, "group": "KPI", "hover": f"KPI: {name} → {parent}"})

    edges = [{"source": parent, "target": name} for name, x, y, color, parent in kpis]

    _draw_network_graph(nodes, edges, "Business Concepts → KPIs", height=580)

    st.divider()

    # ── Semantic mapping table ──
    st.subheader("Semantic Mapping")
    semantic_display = [
        {
            "Business Concept": r["business_concept"],
            "KPI":              r["kpi_name"],
            "Silver Columns":   r["silver_columns"],
            "Transformation":   r["formula"],
            "Business Rule":    r["business_rule"],
        }
        for r in _SEMANTIC_LAYER
    ]
    st.dataframe(pd.DataFrame(semantic_display), width="stretch", hide_index=True)

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

All 17 query_* functions use this CTE — filters applied at the database level, not in memory.
```
""")

