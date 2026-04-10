.PHONY: setup up down run test lint clean

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

setup:
	python -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt
	cp -n .env.example .env || true

up:
	docker compose up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@docker compose exec postgres pg_isready -U $${POSTGRES_USER} || sleep 5

down:
	docker compose down

run:
	$(PYTHON) -m src.pipeline

test:
	$(VENV)/bin/pytest tests/ --cov=src --cov-report=term-missing

lint:
	$(VENV)/bin/ruff check src/ tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .coverage htmlcov coverage.xml
