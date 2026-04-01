"""
AI Chat Module for Healthcare RCM Analytics
============================================

Provides the backend for the AI Assistant tab.

Architecture:
    1. build_system_prompt()  — queries the four meta_* tables (KPI catalog,
                                semantic layer, KG nodes, KG edges) plus the
                                live KPI snapshot from the active dashboard
                                filters to produce the LLM system prompt.
    2. execute_sql_tool()     — safely runs a SELECT/WITH query against the
                                DuckDB database and returns structured results.
    3. run_agentic_turn()     — drives the tool-calling loop:
                                  a. call OpenRouter with TOOL_SCHEMA
                                  b. if the model calls run_sql, execute it
                                     and feed results back
                                  c. loop until the model returns a text reply
                               Yields structured events so the Streamlit UI
                               can show query expanders in real time.

Configuration (.env file in project root):
    OPENROUTER_API_KEY  — required
    OPENROUTER_MODEL    — optional, defaults to openai/gpt-4o-mini
"""

import json
import os
from collections.abc import Iterator

# Load .env file if present — no-op when the file is missing.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Available models for the UI selectbox
# ---------------------------------------------------------------------------
AVAILABLE_MODELS = [
    ("Claude Sonnet 4.6  (Anthropic)", "anthropic/claude-sonnet-4.6"),
    ("GPT-5.4  (OpenAI)", "openai/gpt-5.4"),
    ("Kimi K2.5  (Moonshot)", "moonshotai/kimi-k2.5"),
]

DEFAULT_MODEL = os.environ.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4.6")

# Maximum rows returned to the LLM per query (prevents context overflow).
# Override via AI_MAX_ROWS in .env — see .env.example.
try:
    _MAX_ROWS = max(10, int(os.environ.get("AI_MAX_ROWS", "100")))
except ValueError:
    _MAX_ROWS = 100

# Maximum tool-call iterations per turn (prevents runaway loops).
# Override via AI_MAX_ITERATIONS in .env — see .env.example.
try:
    _MAX_ITERATIONS = max(1, int(os.environ.get("AI_MAX_ITERATIONS", "8")))
except ValueError:
    _MAX_ITERATIONS = 8


# ---------------------------------------------------------------------------
# Tool schema — passed to the OpenRouter API as the `tools` parameter
# ---------------------------------------------------------------------------
TOOL_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Execute a read-only SELECT query against the RCM DuckDB database "
                "and return the results as a table. "
                "Use this to answer specific questions about the data, drill into "
                "breakdowns by payer/department/provider, compute custom metrics, "
                "or look up individual records. "
                f"Results are capped at {_MAX_ROWS} rows — use GROUP BY aggregations "
                "for large datasets."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "A valid DuckDB SELECT statement. "
                            "May use CTEs (WITH … AS). "
                            "Only SELECT and WITH are permitted — no INSERT, UPDATE, "
                            "DELETE, DROP, or other write operations."
                        ),
                    },
                    "description": {
                        "type": "string",
                        "description": "One sentence describing what this query computes.",
                    },
                },
                "required": ["query", "description"],
            },
        },
    }
]


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------


def execute_sql_tool(query: str, db_path=None) -> dict:
    """
    Safely execute a read-only SQL query and return structured results.

    Only SELECT and WITH (CTE) queries are permitted.  Results are capped
    at ``_MAX_ROWS`` rows to avoid flooding the LLM context window.

    Returns a dict with one of two shapes:

    Success::

        {
            "columns":   ["col1", "col2", ...],
            "rows":      [[v, v, ...], ...],   # list of lists
            "row_count": int,                  # rows returned (after cap)
            "total_rows": int,                 # rows before cap
            "truncated": bool,
        }

    Failure::

        {"error": "message"}
    """
    # Safety check — only allow read queries
    # Strip SQL comments (-- line comments and /* block comments */) before checking
    import re

    stripped = re.sub(r"--[^\n]*", "", query)  # remove line comments
    stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL)  # remove block comments
    stripped = stripped.strip().lstrip("(")
    if stripped[:6].upper() not in ("SELECT", "WITH  ", "WITH\n", "WITH\t"):
        first_word = stripped.split()[0].upper() if stripped.split() else ""
        if first_word not in ("SELECT", "WITH"):
            return {"error": "Only SELECT and WITH (CTE) queries are permitted."}

    from src.database import get_connection

    conn = get_connection(db_path, read_only=True)
    try:
        df = conn.execute(query).df()
        total_rows = len(df)
        truncated = total_rows > _MAX_ROWS
        df = df.head(_MAX_ROWS)

        # Convert NaN → None for clean JSON serialisation
        rows = [
            [None if (v != v) else v for v in row]  # NaN check: NaN != NaN
            for row in df.values.tolist()
        ]
        return {
            "columns": list(df.columns),
            "rows": rows,
            "row_count": len(rows),
            "total_rows": total_rows,
            "truncated": truncated,
        }
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        conn.close()


def _format_result_for_llm(result: dict) -> str:
    """Format SQL results as compact CSV for the LLM tool-result message."""
    if "error" in result:
        return f"Query error: {result['error']}"
    if result["row_count"] == 0:
        return "Query returned 0 rows."

    lines = [",".join(str(c) for c in result["columns"])]
    for row in result["rows"]:
        lines.append(",".join("NULL" if v is None else str(v) for v in row))

    note = (
        f"\n[Showing {result['row_count']} of {result['total_rows']} total rows. "
        "Use GROUP BY / aggregations to summarise larger datasets.]"
        if result["truncated"]
        else ""
    )
    return "\n".join(lines) + note


# ---------------------------------------------------------------------------
# Internal: meta-table context string
# ---------------------------------------------------------------------------


def _get_meta_context(db_path=None) -> str:
    """
    Build the data model context block for the AI system prompt.

    Data sources (with graceful fallback):
      - Knowledge graph: Neo4j → DuckDB meta_kg_* → schema introspection
      - Semantic layer:  Cube /meta API → DuckDB meta_semantic_layer
      - KPI definitions: DuckDB meta_kpi_catalog (always)

    Sections:
      1. Valid table names  — from Neo4j entities or meta_kg_nodes
      2. Table schemas      — DESCRIBE per silver table
      3. Join paths         — from Neo4j relationships or meta_kg_edges
      4. Semantic mappings  — from Cube metadata or meta_semantic_layer
      5. KPI definitions    — from meta_kpi_catalog
    """
    from src.database import get_connection

    # ── Knowledge Graph: try Neo4j first, fall back to DuckDB ────────
    try:
        from src.neo4j_client import get_kg_edges, get_kg_nodes

        neo4j_nodes = get_kg_nodes()
        neo4j_edges = get_kg_edges()
    except Exception:
        neo4j_nodes = None
        neo4j_edges = None

    conn = get_connection(db_path, read_only=True)

    if neo4j_nodes:
        nodes = [(n["entity_id"], n["entity_group"], n["silver_table"], n["description"]) for n in neo4j_nodes]
    else:
        try:
            nodes = conn.execute(
                "SELECT entity_id, entity_group, silver_table, description "
                "FROM meta_kg_nodes ORDER BY entity_group, entity_id"
            ).fetchall()
        except Exception:
            nodes = []

    if not nodes:
        live_tables = [
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'silver_%' ORDER BY table_name"
            ).fetchall()
        ]
        nodes = [(t.replace("silver_", ""), "Unknown", t, "") for t in live_tables]

    if neo4j_edges:
        edges = [
            (e["parent_entity"], e["child_entity"], e["join_column"], e["cardinality"], e["business_meaning"])
            for e in neo4j_edges
        ]
    else:
        try:
            edges = conn.execute(
                "SELECT parent_entity, child_entity, join_column, cardinality, business_meaning FROM meta_kg_edges"
            ).fetchall()
        except Exception:
            edges = []

    # ── Semantic Layer: try Cube first, fall back to DuckDB ──────────
    try:
        from src.cube_client import get_semantic_mappings

        cube_mappings = get_semantic_mappings()
    except Exception:
        cube_mappings = None

    if cube_mappings:
        semantic = [
            (m["business_concept"], m["kpi_name"], m["silver_columns"], m["formula"], m["business_rule"])
            for m in cube_mappings
        ]
    else:
        try:
            semantic = conn.execute(
                "SELECT business_concept, kpi_name, silver_columns, formula, business_rule "
                "FROM meta_semantic_layer ORDER BY business_concept"
            ).fetchall()
        except Exception:
            semantic = []

    # ── KPI definitions: always from DuckDB ──────────────────────────
    try:
        kpis = conn.execute(
            "SELECT metric_name, category, definition, formula, benchmark "
            "FROM meta_kpi_catalog ORDER BY category, metric_name"
        ).fetchall()
    except Exception:
        kpis = []

    # Fetch column schemas while connection is still open
    table_columns: dict[str, list[str]] = {}
    for _, _, silver_table, _ in nodes:
        if silver_table:
            try:
                cols = conn.execute(f"DESCRIBE {silver_table}").fetchall()
                table_columns[silver_table] = [f"{c[0]} ({c[1]})" for c in cols]
            except Exception:
                pass

    conn.close()

    lines: list[str] = []

    # ── 1. Valid table names ─────────────────────────────────────────
    valid_tables = [row[2] for row in nodes if row[2]]
    lines.append("## Valid Table Names")
    lines.append("ONLY query tables from this list — never invent or guess a table name:\n" + ", ".join(valid_tables))

    # ── 2. Table schemas ─────────────────────────────────────────────
    lines.append("\n## Table Schemas  (exact column names and types)")
    for _, group, silver_table, desc in nodes:
        if not silver_table:
            continue
        lines.append(f"\n**{silver_table}** ({group}): {desc}")
        if silver_table in table_columns:
            lines.append(f"  Columns: {', '.join(table_columns[silver_table])}")

    # ── 3. Join paths ────────────────────────────────────────────────
    lines.append("\n## Join Paths  (how to connect tables)")
    for parent, child, join_col, cardinality, meaning in edges:
        lines.append(f"- silver_{parent} JOIN silver_{child} ON {join_col} ({cardinality}): {meaning}")

    # ── 4. Semantic mappings ─────────────────────────────────────────
    lines.append("\n## Semantic Mappings  (business concept → KPI → source columns)")
    current_concept = None
    for concept, kpi, cols, formula, rule in semantic:
        if concept != current_concept:
            lines.append(f"\n### {concept}")
            current_concept = concept
        lines.append(f"- **{kpi}**: columns `{cols}` | formula `{formula}` | {rule}")

    # ── 5. KPI definitions ───────────────────────────────────────────
    lines.append("\n## KPI Definitions")
    current_cat = None
    for metric, cat, defn, formula, benchmark in kpis:
        if cat != current_cat:
            lines.append(f"\n### {cat}")
            current_cat = cat
        bench = f" | Benchmark: {benchmark}" if benchmark else ""
        lines.append(f"- **{metric}**: {defn} | Formula: `{formula}`{bench}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public: system prompt builder
# ---------------------------------------------------------------------------


def build_system_prompt(live_kpis: dict | None = None, db_path=None) -> str:
    """
    Build the AI system prompt from meta_* tables plus the live KPI snapshot.

    Args:
        live_kpis: Optional dict of current KPI values from the active
                   dashboard filters, e.g. ``{"Days in A/R": "32.5 days"}``.
        db_path:   Optional DuckDB path override.

    Returns:
        System prompt string ready to be the first message in the list.
    """
    meta = _get_meta_context(db_path)

    kpi_snapshot = ""
    if live_kpis:
        kpi_lines = "\n".join(f"  - {k}: {v}" for k, v in live_kpis.items())
        kpi_snapshot = "\n## Live KPI Snapshot  (from active sidebar filters)\n" + kpi_lines

    return f"""You are an AI analyst embedded in a Healthcare Revenue Cycle Management (RCM) \
Analytics dashboard backed by a DuckDB Medallion-Architecture data warehouse \
(Bronze → Silver → Gold layers), with a Cube semantic layer and Neo4j knowledge graph.

Your role:
- Answer natural-language questions about RCM performance.
- Explain KPIs, formulas, and industry benchmarks clearly.
- Use the run_sql tool to query the database whenever you need specific data \
not already in the KPI snapshot — breakdowns by payer, department, provider, \
time period, denial reason codes, etc.
- Identify issues and suggest actionable next steps.

{meta}
{kpi_snapshot}

## DuckDB SQL syntax (IMPORTANT — this is NOT SQLite or PostgreSQL)
- Date difference: date_diff('day', CAST(start AS DATE), CAST(end AS DATE))
  — do NOT use JULIANDAY, DATEDIFF, or TIMESTAMPDIFF (they don't exist in DuckDB)
- Date formatting: strftime(CAST(col AS DATE), '%Y-%m')
- Date columns in Silver tables are stored as TEXT — always CAST to DATE before date arithmetic
- Current date: CURRENT_DATE
- String functions: UPPER(), LOWER(), TRIM(), CONTAINS(), STARTS_WITH()
- Rounding: ROUND(value, decimals)
- COALESCE(col, default) for NULL handling
- NULLIF(col, '') to convert empty strings to NULL

## Tool usage guidelines
- Prefer aggregation queries (GROUP BY, SUM, COUNT, AVG) over row-level queries.
- Use the silver_* tables for most queries; gold_* views for pre-aggregated KPIs.
- Always include LIMIT when fetching non-aggregated rows.
- Chain multiple tool calls if you need data from several tables.
- After receiving results, interpret them in plain language with context.
- Always use human-readable names instead of IDs in results — use JOINs:
    - Payer name: JOIN silver_payers ON silver_claims.payer_id = silver_payers.payer_id → payer_name
    - Patient name: JOIN silver_patients ON patient_id → first_name, last_name
    - Provider name: JOIN silver_providers ON provider_id → provider_name
  Never GROUP BY or display a raw ID column when the name is available via JOIN.

## Response guidelines
- Be concise — healthcare finance professionals are busy.
- Always state the relevant benchmark when discussing a KPI value.
- Format numbers as "$1.2M", "8.3%", "34 days", etc.
- Never fabricate specific figures — use run_sql to get real data.
- Industry benchmarks (unless overridden in snapshot):
    DAR < 35 days | NCR > 95% | GCR > 70% | Clean Claim Rate > 90%
    Denial Rate < 10% | First-Pass Rate > 85% | Cost to Collect < 3%
"""


# ---------------------------------------------------------------------------
# Public: agentic turn
# ---------------------------------------------------------------------------


def run_agentic_turn(
    messages: list[dict],
    model: str | None = None,
) -> Iterator[dict]:
    """
    Run one conversational turn with an agentic tool-calling loop.

    ``messages`` is mutated in-place — assistant messages, tool-call
    records, and tool-result messages are appended as the loop runs.
    After the generator is exhausted, ``messages`` contains the full
    updated history ready to be persisted for the next turn.

    Yields dicts with a ``"type"`` key:

    ``tool_call``::

        {"type": "tool_call", "description": str, "sql": str}

    ``tool_result``::

        {"type": "tool_result", "description": str, "sql": str,
         "columns": list, "rows": list, "row_count": int,
         "total_rows": int, "truncated": bool, "error": str | None}

    ``text``::

        {"type": "text", "content": str}

    ``error``::

        {"type": "error", "message": str}
    """
    try:
        from openai import OpenAI
    except ImportError:
        yield {"type": "error", "message": "openai package required: pip install openai"}
        return

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key or api_key.strip() in ("", "your_api_key_here"):
        yield {
            "type": "error",
            "message": (
                "OPENROUTER_API_KEY is not configured. Add it to the .env file in the project root and restart the app."
            ),
        }
        return

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
        default_headers={
            "HTTP-Referer": "https://rcm-analytics.local",
            "X-Title": "Healthcare RCM Analytics",
        },
    )

    selected_model = model or DEFAULT_MODEL

    for _iteration in range(_MAX_ITERATIONS):
        response = client.chat.completions.create(
            model=selected_model,
            messages=messages,
            tools=TOOL_SCHEMA,
            tool_choice="auto",
            max_tokens=1500,
            temperature=0.3,
            timeout=60,
        )

        msg = response.choices[0].message
        finish = response.choices[0].finish_reason

        if finish == "tool_calls" or msg.tool_calls:
            # Append assistant message (with tool_calls) to history
            messages.append(
                {
                    "role": "assistant",
                    "content": msg.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments,
                            },
                        }
                        for tc in msg.tool_calls
                    ],
                }
            )

            for tc in msg.tool_calls:
                args = json.loads(tc.function.arguments)
                sql = args.get("query", "")
                desc = args.get("description", "Running query…")

                yield {"type": "tool_call", "description": desc, "sql": sql}

                result = execute_sql_tool(sql)

                yield {
                    "type": "tool_result",
                    "description": desc,
                    "sql": sql,
                    "columns": result.get("columns", []),
                    "rows": result.get("rows", []),
                    "row_count": result.get("row_count", 0),
                    "total_rows": result.get("total_rows", 0),
                    "truncated": result.get("truncated", False),
                    "error": result.get("error"),
                }

                # Feed result back into conversation history
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": _format_result_for_llm(result),
                    }
                )

        else:
            # Model returned a final text response — append and yield
            content = msg.content or ""
            messages.append({"role": "assistant", "content": content})
            yield {"type": "text", "content": content}
            return

    yield {"type": "error", "message": f"Exceeded {_MAX_ITERATIONS} tool-call iterations."}
