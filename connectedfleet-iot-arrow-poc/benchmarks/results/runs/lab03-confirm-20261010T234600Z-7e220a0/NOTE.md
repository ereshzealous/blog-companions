# Stopped in E1: the host guard missed short-lived processes

This run (7e220a0) waited for quiet windows as designed (11 waits, 2 discarded attempts) while the laptop's owner ran a
large Rust test suite (dozens of short-lived test binaries; load average 6–16). The guard sampled per-process CPU every
2 s, so processes that started and ended between samples were invisible: accepted cases still ran slow (row worker
66–80K and Arrow worker 83–96K wall rows/s, against about 120K and 145K on a quiet host). Stopped at 00:14Z after 13
cases; no manifest was checkpointed; nothing from it is cited. The guard now measures total host CPU minus Docker's
VM, Docker Desktop and the harness itself (psutil), which includes short-lived processes.
