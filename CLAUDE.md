# CLAUDE.md — Project Guide for Healthcare RCM Analytics

This file is automatically read by Claude Code at the start of every session.
Follow these rules whenever modifying this project.

---

## Architecture Overview

```
CSV files → Bronze (raw TEXT) → Silver (typed + FK) → Gold (aggregated views) → Dashboard
                                        ↓                        ↓
                                  Cube Semantic Layer    Neo4j Knowledge Graph
                                  (measures/dims/joins)  (10 entities, 9 FKs)
                                        ↓                        ↓
                                  meta_* tables (DuckDB fallback) ←→ AI Assistant
```

**Key modules:**
| File | Responsibility |
|------|----------------|
| `src/database.py` | DuckDB schema definitions, ETL pipeline, `build_filter_cte()`, schema migration |
| `src/metrics.py` | All 26 `query_*` KPI functions + `FilterParams` dataclass (Cube → DuckDB fallback) |
| `src/cube_client.py` | Cube REST API client — semantic metadata + metric queries |
| `src/neo4j_client.py` | Neo4j Cypher client — knowledge graph nodes + edges |
| `src/metadata_pages.py` | Five sidebar metadata pages — sourced from Cube/Neo4j with DuckDB fallback |
| `src/ai_chat.py` | AI tab backend: `build_system_prompt()`, `execute_sql_tool()`, `run_agentic_turn()` |
| `src/validators.py` | SQL COUNT-based data quality assertions |
| `app.py` | Streamlit app: 12 tabs + sidebar + metadata page router |
| `generate_sample_data.py` | Creates CSV files **and** inserts rows into all four `meta_*` tables |

---

## Single Source of Truth

Follow this table strictly. Never duplicate data across two locations.

| Information | Single source | Do NOT also edit |
|-------------|---------------|-----------------|
| KPI definitions, formulas, categories, benchmarks | `meta_kpi_catalog` table (populated in `generate_sample_data.py`) | `README.md` Metrics Reference (update the count only) |
| Business concept → KPI → column mappings | Cube model YAML files (`cube/model/`) → DuckDB `meta_semantic_layer` fallback | — |
| Silver-layer entity descriptions | Neo4j Entity nodes → DuckDB `meta_kg_nodes` fallback | `_KG_NODES` in `metadata_pages.py` (static fallback) |
| Entity relationships / foreign keys | Neo4j HAS_FK relationships → DuckDB `meta_kg_edges` fallback | — |
| Knowledge graph node **positions** (x, y) | `_KG_NODES` list in `metadata_pages.py` | — (layout only, not in DB) |
| Table catalog (Bronze / Silver / Gold) | `_TABLE_CATALOG` list in `metadata_pages.py` | — |
| Dashboard tab list and descriptions | `README.md` | — |
| Test count | `README.md` (verify before updating) | — |

---

## What Updates Automatically (No Manual Editing Needed)

When you modify any of these, the listed pages/features update on the next page load:

| What you change | Auto-updates |
|-----------------|-------------|
| Row in `meta_kpi_catalog` | Data Catalog page KPI table, AI system prompt |
| Row in `meta_semantic_layer` | Semantic Layer page mapping table, AI system prompt |
| Row in `meta_kg_edges` | Knowledge Graph page relationships table + graph edges, AI system prompt |
| Row in `meta_kg_nodes` | Knowledge Graph page node hover text, AI system prompt |

---

## Adding or Changing an Environment Variable

1. Add the variable to `.env.example` with:
   - A comment explaining what it does
   - The default value (shown in the commented-out example)
   - The range of valid values or format
2. Update the **Configuration Reference** table in `README.md`
3. Update the code that reads it (add `os.environ.get("VAR", "default")`)
4. **Never** hard-code a value that might need to differ between environments

> `.env.example` is the contract between the codebase and the operator.
> Every env var the code reads must appear there.

---

## Common Change Recipes

### Adding a New KPI

1. Add a row to `meta_kpi_catalog` in `generate_sample_data.py`
2. Add the matching row to `meta_semantic_layer` in `generate_sample_data.py`
3. Write a `query_*` function in `src/metrics.py`
4. Call it in the relevant tab in `app.py`
5. Update `README.md`: increment the KPI count in the Metrics Reference intro sentence, add a row to the table
6. Write at least one unit test in `tests/test_metrics.py`
7. ✅ Data Catalog, Semantic Layer, and AI system prompt update automatically — no other edits needed

### Adding a New Dashboard Tab

1. Add the tab label to `st.tabs([...])` in `app.py` (currently 12 tabs)
2. Write the `with tabN:` block in `app.py`
3. Update `README.md`:
   - Change "twelve analytical tabs" → count + 1 in the Overview paragraph
   - Add a row to the Overview tab table
   - Add a `### Tab N — Title` section under Dashboard Tabs
4. Update `DASH_TABS` in `render_data_lineage()` in `metadata_pages.py` if the tab should appear in the lineage diagram

### Adding a New Silver-Layer Data Entity

1. Add the schema to `src/database.py` (CREATE TABLE + Silver ETL)
2. Add a CSV generator section to `generate_sample_data.py`
3. Insert a row into `meta_kg_nodes` in `generate_sample_data.py`
4. Insert rows into `meta_kg_edges` for each foreign key relationship
5. Add a position entry to `_KG_NODES` in `src/metadata_pages.py` (x, y layout coordinates)
6. Add entries to `_TABLE_CATALOG` in `src/metadata_pages.py` (Bronze + Silver rows)
7. Update `TABLE_ORDER` in `render_data_lineage()` in `metadata_pages.py`
8. ✅ Knowledge Graph edges, Semantic Layer, and AI prompt update automatically

### Modifying the AI Assistant (`src/ai_chat.py`)

- If you change the tool schema or tool loop behaviour, update the **AI Architecture** metadata page (`render_ai_architecture()` in `metadata_pages.py`)
- If you add a new model option, add it to `AVAILABLE_MODELS` in `ai_chat.py`
- The system prompt is built dynamically — no manual updates needed when the meta_* tables change

### Updating the README

Update `README.md` whenever:
- Tab count changes (Overview paragraph + tab table)
- Metadata page count changes (Overview paragraph + Metadata Pages section)
- Test count changes (Running Tests section — verify with `pytest tests/ -q | tail -1`)
- A new dependency is added (`requirements.txt` → Dependencies table in README)
- Setup steps change

---

## Test Requirements

```bash
pytest tests/ -q          # must pass with 0 failures before every commit
pytest tests/ -q | tail -1  # shows the count — currently 334 passed
```

- `tests/test_metrics.py` — 151 tests covering all `query_*` functions in `src/metrics.py`
- `tests/test_validators.py` — 40 tests covering all validators in `src/validators.py`
- `tests/test_etl_pipeline.py` — 22 tests covering `load_csv_to_bronze()` and `_etl_bronze_to_silver()` in `src/database.py`
- `tests/test_ai_chat_sql.py` — 22 tests covering `execute_sql_tool()` and `_format_result_for_llm()` in `src/ai_chat.py`
- `tests/test_data_loader.py` — 19 tests covering `_parse_dates`, `_parse_booleans`, `_validate_columns`, `load_all_data`, `load_gold_data` in `src/data_loader.py`
- `tests/test_ai_chat_prompt.py` — 15 tests covering `build_system_prompt()` and `_get_meta_context()` in `src/ai_chat.py`
- `tests/test_database.py` — 15 tests covering `build_filter_cte()` in `src/database.py`
- `tests/test_app_utils.py` — 13 tests covering `df_to_csv`, `dfs_to_excel`, `_linear_forecast` from `app.py`
- `tests/test_ai_chat_agentic.py` — 11 tests covering `run_agentic_turn()` tool-calling loop in `src/ai_chat.py`
- `tests/test_ai_chat_config.py` — 10 tests covering `AI_MAX_ROWS` and `AI_MAX_ITERATIONS` env var parsing in `src/ai_chat.py`
- `tests/test_cube_client.py` — 9 tests covering `is_cube_available()`, `query_cube()`, `build_cube_filters()` in `src/cube_client.py`
- `tests/test_neo4j_client.py` — 7 tests covering `is_neo4j_available()`, `get_kg_nodes()`, `get_kg_edges()` in `src/neo4j_client.py`

Every new `query_*` function **must** have at least two unit tests (happy path + edge case with empty data).

---

## Environment Setup

```bash
pip install -r requirements.txt
python generate_sample_data.py   # generates data/ CSVs + rcm_analytics.db
streamlit run app.py             # starts at http://localhost:8501
```

**AI tab:** create `.env` in the project root:
```
OPENROUTER_API_KEY=your_key_here
OPENROUTER_MODEL=openai/gpt-4o-mini   # optional override
```
The `.env` file is not committed. All other tabs work without it.

---

## Code Conventions

See `.claude/skills/standards.md` for the complete reference.

---

## Development Workflow

Every non-trivial change follows this 6-stage process. See `.claude/skills/feature-workflow.md` for full details.

```
1. PLAN → 2. APPROVE → 3. CODE → 4. VERIFY → 5. REVIEW → 6. DEPLOY
```

| Skill / Command | When | What it does |
|----------------|------|-------------|
| `/feature-workflow` | Starting any feature | Orchestrates all 6 stages (includes verification gates + pre-commit review) |
| `/standards` | During coding + review | Project conventions reference |
| `/review-pr` | After PR is created | Multi-agent PR review (tests, errors, comments, code quality) |

**Quick commands:**
```bash
make test      # run pytest
make lint      # run ruff
make verify    # test + lint (fast gate)
```
