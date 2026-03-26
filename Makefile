# Healthcare RCM Analytics — Development Commands
# ================================================
# Usage: make <target>

.PHONY: test lint verify format setup data

# ── Quality Gates ────────────────────────────────────────────
test:
	python -m pytest tests/ -q

lint:
	python -m ruff check src/ tests/ app.py generate_sample_data.py

verify: lint test
	@echo ""
	@echo "✅ All gates passed (lint + tests)"

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
