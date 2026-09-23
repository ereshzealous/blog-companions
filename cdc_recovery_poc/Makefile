# CDC failure-recovery lab.
#   make all        capacity -> recovery -> capture pressure -> history loss, then tear down
#   make up         fresh environment, seeded, connector registered
#   make verify     check the published runs in results/ without starting anything
#   make down-all   stop everything and delete the volumes
# The fault scenarios need LAMBDA (change rate) and MU (measured sink capacity) from the capacity run.
.PHONY: all up capacity recovery pressure history-loss verify down clean down-all help
.DEFAULT_GOAL := help

LAMBDA ?= 20000
MU     ?= 40000

all: capacity recovery pressure history-loss down-all ## every scenario, then teardown

up: ## fresh environment: volumes destroyed, seeded, connector registered, snapshot complete
	scenarios/00-up.sh setup

capacity: ## measure capture and sink capacity before any fault; writes results/capacity-<UTC>
	scenarios/01-capacity.sh

recovery: ## schema change, throttle, worker kill, sink crash, deletes; results/recovery-<UTC>
	LAMBDA=$(LAMBDA) MU=$(MU) scenarios/02-recovery-test.sh

pressure: ## Kafka paused while writes continue; results/capture-pressure-<UTC>
	LAMBDA=$(LAMBDA) scenarios/03-capture-pressure.sh

history-loss: ## expected failure: the slot's history is removed; results/history-loss-<UTC>
	LAMBDA=$(LAMBDA) scenarios/04-history-loss.sh

verify: ## check the four published runs against their pass criteria (no Docker needed)
	scripts/verify-run.py --published

down: ## stop and remove containers, keep volumes
	docker compose down --remove-orphans

down-all: ## stop and remove containers and volumes (all lab data)
	docker compose down -v --remove-orphans

clean: ## remove runtime state; captured runs under results/ are kept
	rm -rf control __pycache__ lab/__pycache__

help:
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-13s %s\n", $$1, $$2}'
