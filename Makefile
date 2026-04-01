# Healthcare RCM Analytics — Development Commands
# ================================================
# Usage: make <target>

.PHONY: test lint verify format coverage security ci setup data run run-full

# ── Quality Gates ────────────────────────────────────────────
test:
	python -m pytest tests/ -q

lint:
	python -m ruff check src/ tests/ app.py generate_sample_data.py

verify: lint test
	@echo ""
	@echo "✅ All gates passed (lint + tests)"

# ── Coverage & Security ─────────────────────────────────────
coverage:
	python -m pytest tests/ -q --cov=src --cov-report=term-missing

security:
	bandit -r src/ app.py -c bandit.toml --severity-level medium
	pip-audit -r requirements.txt

ci: lint test security
	@echo ""
	@echo "✅ All CI gates passed (lint + tests + security)"

# ── Formatting ───────────────────────────────────────────────
format:
	python -m ruff check --fix src/ tests/ app.py generate_sample_data.py
	python -m ruff format src/ tests/ app.py generate_sample_data.py

# ── Setup ────────────────────────────────────────────────────
setup:
	pip install -r requirements.txt

data:
	python generate_sample_data.py

# ── Run ──────────────────────────────────────────────────────
run:
	streamlit run app.py

run-full:
	docker compose up -d
	streamlit run app.py
