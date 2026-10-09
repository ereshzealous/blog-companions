.PHONY: all up test run console benchmark surge report down-all

all:
	$(MAKE) up
	$(MAKE) test
	$(MAKE) run

up:
	docker compose up -d --wait

test:
	./gradlew test
	python3 -m unittest discover -s tools -p test_benchmark_console.py

run:
	./gradlew bootRun

console:
	python3 tools/benchmark_console.py serve

benchmark:
	./gradlew jmh

surge:
	./gradlew surgeBenchmark

report:
	python3 tools/benchmark_console.py export --input benchmarks/results --output build/reports/benchmarks/saved.html

down-all:
	docker compose down --remove-orphans
