# ConnectedFleet POC — one repository, three labs.
# Every target either does verified work or fails with a clear message.
# Written for GNU Make 3.81+ (the macOS default).

SHELL := /bin/bash
.DEFAULT_GOAL := help

UV ?= uv
COMPOSE ?= docker compose
# Lab 01 profile: quick | default | full (see src/connectedfleet/lab01/orchestrate.py)
LAB01_PROFILE ?= default
# Lab 03 profile: smoke | full (benchmarks/workloads/<profile>.yaml)
PROFILE ?= smoke
# Infrastructure services started by `make up` (pipeline/lane processes are started by the labs).
INFRA := kafka connect postgres mongodb clickhouse
ALL_PROFILES := --profile pipeline --profile lanes --profile tools

.PHONY: results-html help doctor sync lab-01 test verify-lab-01 verify-all \
        up down init seed status smoke logs lab-02 lab-03 benchmark report clean data-dirs

help: ## Show targets and what they touch
	@echo "ConnectedFleet POC targets:"
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_ .-]+:.*## / {printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

doctor: ## Read-only host check: tools, Python 3.12, Docker access, ports, memory
	@./scripts/doctor.sh

sync: ## Create/refresh .venv (Python 3.12) exactly from uv.lock
	$(UV) sync --frozen

lab-01: ## Lab 01: row vs Arrow parity + per-phase timings, no Docker (LAB01_PROFILE=quick|default|full)
	$(UV) run --frozen python labs/lab01_memory_boundary.py --profile $(LAB01_PROFILE)

test: ## Unit tests only (no Docker); integration tests are marked and excluded
	$(UV) run --frozen pytest tests/unit -m "not integration and not slow"

verify-lab-01: ## Gate 1 technical checks: unit tests, then a quick Lab 01 run
	$(MAKE) test
	$(MAKE) lab-01 LAB01_PROFILE=quick

# ---------------------------------------------------------------- Docker stack (Lab 02 / Lab 03)
.env:
	@cp .env.example .env
	@echo "created .env from .env.example (local-only credentials; edit if needed)"

data-dirs:
	@mkdir -p .data/metrics .data/work
	@chmod 777 .data/metrics .data/work

up: .env data-dirs ## Build images and start infrastructure; returns only when every service is healthy
	$(COMPOSE) up -d --build --wait $(INFRA)

down: ## Stop every ConnectedFleet container; data stays (containers and volumes are kept)
	$(COMPOSE) $(ALL_PROFILES) stop

init: .env ## Idempotent: topics, ClickHouse tables (3 lane DBs), Mongo users + seed, register/verify connectors, versions.json
	$(UV) run --frozen python -m connectedfleet.ops init

seed: .env ## Deterministic source data (idempotent upserts; PostgreSQL rows come from infra/postgres on first start)
	$(UV) run --frozen python -m connectedfleet.ops seed

status: .env ## Connector states, Kafka topic sizes, row counts per lane database
	$(UV) run --frozen python -m connectedfleet.ops status

smoke: .env data-dirs ## Lab 02 with small volumes (same checks); needs `make up init`
	$(UV) run --frozen python labs/lab02_end_to_end.py --smoke

lab-02: .env data-dirs ## Lab 02: real CDC/outbox/Mongo/telemetry/CSV -> canonical -> A/B/C, correctness + faults; needs `make up init`
	$(UV) run --frozen python labs/lab02_end_to_end.py

logs: ## Follow container logs (SERVICE=<name> to filter)
	$(COMPOSE) $(ALL_PROFILES) logs -f --tail=200 $(SERVICE)

lab-03: .env data-dirs ## Lab 03 benchmarks E1-E4 (+E5 if installed), PROFILE=smoke|confirm|full; needs `make up init` and a passing Lab 02
	$(COMPOSE) build app  # workers run this image: rebuild so it matches the working tree the run records
	$(UV) run --frozen python labs/lab03_experiments.py --profile $(PROFILE)

benchmark: lab-03 ## Same as lab-03 (uses PROFILE)

report: ## Re-validate every saved Lab 03 run and regenerate aggregate CSVs + summary (no Docker needed)
	$(UV) run --frozen python benchmarks/report.py

results-html: ## Results pages: benchmarks/results/benchmark-results.html (every use case, every run) + runs/<id>/report.html
	$(UV) run --frozen python -m connectedfleet.bench.results_html

verify-all: verify-lab-01 ## All gates: unit tests, Lab 01, integration tests, Lab 02, Lab 03 smoke (needs `make up init`)
	$(UV) run --frozen pytest tests/integration -m integration
	$(MAKE) lab-02
	$(MAKE) lab-03 PROFILE=smoke

clean: ## Delete Python caches; then OPTIONALLY delete all containers + volumes (asks; type 'delete')
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache
	@if [ -t 0 ]; then \
	  echo "Also delete ALL ConnectedFleet containers and volumes (Kafka, PostgreSQL, MongoDB, ClickHouse data)?"; \
	  read -r -p "Type 'delete' to confirm, anything else to keep them: " answer; \
	  if [ "$$answer" = "delete" ]; then $(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans; \
	  else echo "Docker state kept."; fi; \
	else echo "Non-interactive shell: Docker containers and volumes kept."; fi

.PHONY: public-report
public-report: ## Public reading page from saved evidence only; no stack, tests or new measurements
	python3 tools/build_public_report.py
