# Stopped during E1 warm-ups: 20-second quiet windows were not enough

This run (d1904b5) used the whole-host CPU guard. While a large Rust test suite ran on the laptop (bursts of 8–12
cores and heavy disk I/O), the guard waited 20 times and discarded 9 attempts in 26 minutes, and still the cases it
accepted ran slow (row worker 74K, Arrow worker 90K wall rows/s against about 120K and 145K on a quiet host): short
quiet windows between bursts do not mean the disk and caches have settled, and disk I/O cannot be attributed to a
process on macOS. Stopped at 00:46Z after four warm-ups; nothing from it is cited. The confirm profile now asks for
60 quiet seconds with other work under one core before each case, and re-runs a case if other work exceeded one core.
