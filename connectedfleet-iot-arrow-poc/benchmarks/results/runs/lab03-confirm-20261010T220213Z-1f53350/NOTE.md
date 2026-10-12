# Stopped in E1: native host load during about half of the timing cases

This confirm-profile run (code as 055e989) started on an idle host at 22:02Z (no foreign containers, no native
process above 50% CPU, load under 6), then the laptop's owner started other work: Rust compiles (`rustc`, 100–800%
CPU, 22:07–22:12Z) and a native benchmark (`target/release/t14`, 240–1,200% CPU, from 22:15Z). host-load.txt has
the 30-second samples. Rates of the same lane moved with that load (row worker 92–132K wall rows/s across
repetitions). It was stopped at 22:28Z in the live-latency part of E1; no manifest was checkpointed and nothing
from it is cited.

A start-of-run idle check cannot protect a 75-minute run on a shared laptop. Fixed in the next commit (D-032): the
harness waits for a quiet host before every timing case, samples native load while the case runs, re-runs a case
that overlapped other work, and the validator rejects a case whose last attempt was still busy.
