"""Lab 03 entry point: `make lab-03 PROFILE=smoke|full`.

Controlled A/B/C experiments on the same POC: E1 direct ingestion (drain throughput
and live latency), E2 transformation isolation (B vs C plus the 2x2 compute x insert
format matrix), E3 CSV backfill (direct vs row vs Arrow), E4 correctness and
failure behaviour, E5 optional interoperability. Prerequisites: `make up init` and a
passing `make lab-02`. Workloads: benchmarks/workloads/<profile>.yaml.
"""

from connectedfleet.bench.run import main

if __name__ == "__main__":
    raise SystemExit(main())
