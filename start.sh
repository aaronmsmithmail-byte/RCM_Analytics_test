#!/usr/bin/env bash
# ============================================================
# Healthcare RCM Analytics — Full Stack Launcher
# ============================================================
# Usage:
#   ./start.sh           # Full stack (Cube + Neo4j + Streamlit)
#   ./start.sh --local   # Streamlit only (DuckDB fallback)
# ============================================================

set -e

# ── Navigate to project root (where this script lives) ──────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
echo "📁 Working directory: $SCRIPT_DIR"

# ── Create/activate virtual environment ──────────────────────
if [ ! -d "venv" ]; then
    echo "🐍 Creating virtual environment..."
    python3 -m venv venv
fi

echo "🐍 Activating virtual environment..."
source venv/bin/activate

# ── Install dependencies ─────────────────────────────────────
echo "📦 Installing dependencies..."
pip install -q -r requirements.txt

# ── Generate sample data (if not already present) ────────────
if [ ! -f "data/rcm_analytics.db" ]; then
    echo "🗄️  Generating sample data..."
    python generate_sample_data.py
else
    echo "🗄️  Database already exists — skipping data generation"
fi

# ── Launch Docker services (unless --local flag) ─────────────
if [ "$1" != "--local" ]; then
    if command -v docker &> /dev/null && docker info &> /dev/null 2>&1; then
        echo "🐳 Starting Cube + Neo4j via Docker Compose..."
        docker compose up -d
        echo "   Cube:    http://localhost:4000"
        echo "   Neo4j:   http://localhost:7474"
    else
        echo "⚠️  Docker not available — running in local mode (DuckDB fallback)"
    fi
else
    echo "🏠 Local mode — skipping Docker services"
fi

# ── Launch Streamlit ─────────────────────────────────────────
echo ""
echo "🚀 Launching Streamlit dashboard..."
echo "   Dashboard: http://localhost:8501"
echo ""
streamlit run app.py
