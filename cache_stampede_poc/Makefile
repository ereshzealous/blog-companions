# Cache stampede POC — two tiers.
#   make all       Tier 1 (no Docker) then Tier 2 (Docker)
#   make tier1     deterministic simulation + tests, a few seconds
#   make tier2     live Redis 7.4 + PostgreSQL 16 lab, a few minutes
#   make down-all  tear the lab down and remove its volumes
.PHONY: all tier1 tier2 down-all clean help
.DEFAULT_GOAL := help

PY      ?= python3
COMPOSE := docker compose -f live/docker-compose.yml

all: tier1 tier2 ## run both tiers

tier1: ## 8 scenarios + 16 pytest cases, writes captured-output/
	$(PY) run.py --scenario all --save
	$(PY) -m pytest -q tests

tier2: ## step 0 -> teardown, writes reports/ and reports/REPORT.md
	./run_all.sh

down-all: ## stop the lab and delete its volumes
	$(COMPOSE) down -v --remove-orphans

clean: ## remove local caches (captured evidence is kept)
	rm -rf .pytest_cache stampede/__pycache__ tests/__pycache__

help:
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-10s %s\n", $$1, $$2}'
