# Interference: do not cite this run

Recorded 2026-10-10. During this run, two containers unrelated to ConnectedFleet were busy on the
same host (`docker stats` right after the run: one at ~900% CPU, one at ~380% CPU). Every phase on
both paths ran ~2–2.5× slower than the clean run `lab01-20261010T162903Z-nogit`, and the Arrow
path's phases slowed more under CPU contention than the row path's.

Parity checks still passed, so the run is kept as evidence of correctness under load, but its
timings are not comparable and are not used in any article or figure. Re-run `make lab-01` on a quiet host.
