---
name: standards
description: >
  Project standards and conventions for the Healthcare RCM Analytics codebase.
  Reference this document when reviewing code, creating new modules, or validating
  changes. Use when the user says "check standards", "validate conventions",
  "naming check", "does this follow our standards", or during any review phase.
---

# Project Standards & Conventions

All code changes must conform to these standards. Review agents should validate
changes against each applicable section.

---

## 1. Data Modeling Standards

### Table Naming (Medallion Architecture)
| Layer | Prefix | Example | Purpose |
|-------|--------|---------|---------|
| Bronze | `bronze_*` | `bronze_claims` | Raw CSV ingestion, all TEXT columns |
| Silver | `silver_*` | `silver_claims` | Typed, FK-constrained, validated |
| Gold | `gold_*` | `gold_monthly_kpis` | SQL VIEWs — pre-aggregated KPIs |
| Metadata | `meta_*` | `meta_kpi_catalog` | AI-queryable semantic/KG tables |
| Pipeline | `pipeline_*` | `pipeline_runs` | ETL tracking and data freshness |

### Column Naming
| Pattern | Convention | Examples |
|---------|-----------|----------|
| Primary keys | `{entity}_id` | `claim_id`, `patient_id`, `payer_id` |
| Foreign keys | Same as PK it references | `payer_id` in `silver_claims` → `silver_payers.payer_id` |
| Dates | `{action}_date` or `date_of_{noun}` | `submission_date`, `date_of_service`, `payment_date` |
| Money | `{descriptor}_amount` | `charge_amount`, `payment_amount`, `denied_amount` |
| Booleans | `is_{property}` | `is_clean_claim`, `is_accurate_payment` |
| Percentages | `{property}_pct` or `{property}_rate` | `avg_reimbursement_pct` |
| Metadata | `_{name}` (underscore prefix) | `_loaded_at` |
| All columns | `snake_case` | Never camelCase or PascalCase |

### Type Conventions
| Layer | Column type | DuckDB type |
|-------|------------|-------------|
| Bronze | Everything | `TEXT` |
| Silver | IDs, codes, dates | `TEXT` |
| Silver | Money, percentages | `REAL` |
| Silver | Counts, booleans | `INTEGER` |
| Silver | Timestamps | `TIMESTAMP` |

### SQL Conventions
- Parameterized `?` placeholders — **never** f-strings with user input
- All metric queries use `build_filter_cte(fp)` for consistent filtering
- Date formatting: `strftime(CAST(col AS DATE), '%Y-%m')`
- Date arithmetic: `date_diff('day', CAST(d1 AS DATE), CAST(d2 AS DATE))`
- Boolean conversion from text: `CASE UPPER(TRIM(col)) WHEN 'TRUE' THEN 1 WHEN '1' THEN 1 WHEN 'YES' THEN 1 ELSE 0 END`
- Empty string → NULL: `NULLIF(TRIM(COALESCE(col, '')), '')`
- NULL PKs filtered: `WHERE pk IS NOT NULL AND pk != ''`

---

## 2. Python Naming Standards

### Modules
- Lowercase with underscores: `cube_client.py`, `data_loader.py`, `metadata_pages.py`
- One module per logical domain — don't overload a single file

### Functions
| Prefix | Purpose | Example |
|--------|---------|---------|
| `query_*` | KPI metric queries | `query_denial_rate(p, db_path=None)` |
| `get_*` | Data retrieval (non-metric) | `get_connection()`, `get_kg_nodes()` |
| `is_*` / `has_*` | Boolean checks | `is_cube_available()`, `has_medallion_schema()` |
| `build_*` | Construct complex objects | `build_filter_cte()`, `build_system_prompt()` |
| `load_*` | Load data from storage | `load_all_data()`, `load_csv_to_bronze()` |
| `render_*` | Streamlit page rendering | `render_knowledge_graph()` |
| `_private` | Module-internal helpers | `_cte()`, `_empty_trend()`, `_try_cube_query()` |
| `seed_*` | Initialize external stores | `seed_knowledge_graph()` |

### Classes
- PascalCase: `FilterParams`, `TestQueryDenialRate`
- Dataclasses for value objects: `@dataclass class FilterParams`
- Test classes: `Test{ComponentName}`

### Constants
- Module-level public: `SCREAMING_SNAKE_CASE` — `DB_PATH`, `CUBE_API_URL`, `TOOL_SCHEMA`
- Module-level private: `_SCREAMING_SNAKE_CASE` — `_MAX_ROWS`, `_HEALTH_TTL`, `_KG_NODES`

### Variables
- Local: `snake_case`
- Domain abbreviations OK: `df` (DataFrame), `conn` (connection), `p` (FilterParams), `sql`
- Loop variables: `i`, `row`, `t` (table), `n` (node)

---

## 3. Package & Import Standards

### Import Order (enforced by ruff isort)
```python
# 1. Standard library
import os
import time
from dataclasses import dataclass

# 2. Third-party packages
import duckdb
import numpy as np
import pandas as pd

# 3. Local modules (first-party = src)
from src.database import build_filter_cte, query_to_dataframe
```

### Optional Dependencies
```python
try:
    from neo4j import GraphDatabase
    _HAS_NEO4J = True
except ImportError:
    _HAS_NEO4J = False
```

### Lazy Imports (when module may not be available)
```python
def _try_cube_query(...):
    try:
        from src.cube_client import query_cube, is_cube_available
        ...
    except Exception:
        return None
```

### Environment Variables
- Load `.env` at module level before constants:
  ```python
  try:
      from dotenv import load_dotenv
      load_dotenv()
  except ImportError:
      pass
  ```
- Read with defaults: `VAR = os.environ.get("VAR_NAME", "default_value")`
- Type-safe parsing:
  ```python
  try:
      _MAX_ROWS = max(10, int(os.environ.get("AI_MAX_ROWS", "100")))
  except ValueError:
      _MAX_ROWS = 100
  ```

---

## 4. Client Module Standards

Every external service client (`src/*_client.py`) must follow this pattern:

```python
"""
Service Name Client
====================
One-line description.

Gracefully returns None when service is unavailable so the app
falls back to DuckDB meta_* tables.

Environment variables:
    SERVICE_URL — description (default: ...)
"""

import os
import time

SERVICE_URL = os.environ.get("SERVICE_URL", "default")

_health_cache = {"available": None, "checked_at": 0}
_HEALTH_TTL = 60  # seconds


def is_service_available() -> bool:
    """Check if service is reachable. Result cached for 60 seconds."""
    now = time.time()
    if now - _health_cache["checked_at"] < _HEALTH_TTL and _health_cache["available"] is not None:
        return _health_cache["available"]
    try:
        # ... check connectivity ...
        _health_cache["available"] = True
    except Exception:
        _health_cache["available"] = False
    _health_cache["checked_at"] = now
    return _health_cache["available"]


def get_data():
    """Returns structured data, or None if unavailable."""
    if not is_service_available():
        return None
    try:
        # ... query service ...
        return result
    except Exception:
        return None
```

---

## 5. Testing Standards

### File & Naming
- One test file per source module: `tests/test_{module}.py`
- Test classes: `Test{Component}` — group related tests
- Test methods: `test_{action}_{expectation}`
  - Good: `test_missing_csv_skips_gracefully`, `test_denial_rate_non_negative`
  - Bad: `test_1`, `test_function`, `test_it_works`

### Fixtures
```python
@pytest.fixture
def db(tmp_path):
    """Temporary DuckDB database with Silver-layer data."""
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    # ... insert test data ...
    conn.close()
    return db_path
```

### Required Coverage
- Every `query_*` function: minimum 2 tests (happy path + empty data)
- Every new public function: at least 1 test
- Client modules: test unavailable → returns None, test mocked success

### Assertions
- Use pytest `assert` (not unittest methods)
- Floats: `assert val == pytest.approx(expected, abs=0.1)`
- DataFrames: `assert df.empty`, `assert "column" in df.columns`
- Dicts: `assert "error" in result`, `assert result["key"] == expected`

---

## 6. Documentation Standards

### Docstrings (Google style)
```python
def query_denial_rate(p: FilterParams, db_path=None):
    """Calculate Claim Denial Rate.

    Denial Rate = (Denied + Appealed Claims) / Total Claims * 100

    Args:
        p:       FilterParams with date range and optional filters.
        db_path: Optional DuckDB path override for testing.

    Returns:
        tuple: (denial_rate_percentage, trend_dataframe)
    """
```

### Section Headers
```python
# ── Section Name ──────────────────────────────────────────────
# ===========================================================================
# MAJOR SECTION TITLE
# ===========================================================================
```

### Comments
- Comment the **why**, not the **what**
- Good: `# TTL-cached to avoid hammering unavailable services`
- Bad: `# increment counter by 1`
- No commented-out code in production files

### .env.example
Every env var the code reads must appear in `.env.example` with:
- A comment explaining what it does
- The default value
- Valid value range or format

---

## 7. Linting Standards (ruff)

### Configuration: `ruff.toml`
- Target: Python 3.11, line-length 120
- Rules: E, W, F, I, UP, B, SIM, T20
- Run: `ruff check src/ tests/ app.py generate_sample_data.py`
- Must pass with zero violations before every commit

### Key Rules
- **F401**: No unused imports (use `# noqa: F401` only when import is needed by monkeypatch/re-export)
- **I001**: Imports must be sorted (stdlib → third-party → local)
- **T201**: No `print()` in `src/` modules (use proper logging or return values)
- **B**: No mutable default arguments, no bare `except:`

---

## 8. Security Standards

- SQL injection: **Always** use parameterized queries (`?` placeholders)
- `execute_sql_tool()`: Rejects non-SELECT/WITH queries before execution
- API keys: Never hardcoded — always from env vars via `os.environ.get()`
- `.env` files: Listed in `.gitignore`, never committed
- Docker secrets: Passed via environment variables in `docker-compose.yml`
- Row limits: AI queries capped at `_MAX_ROWS` (default 100) to prevent context overflow
- Iteration limits: Tool-calling loop capped at `_MAX_ITERATIONS` (default 8) to prevent runaway loops
