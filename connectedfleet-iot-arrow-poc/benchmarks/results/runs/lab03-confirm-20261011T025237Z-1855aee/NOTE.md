# Stopped after one warm-up: kernel time from our own stack counted as other work

This run (1855aee) discarded a tuned-sink warm-up at 349% 'other' CPU. Whole-host system time includes the kernel's work for Docker's VM (disk and network), which belongs to no readable process. The guard now compares user CPU only (other people's builds, tests and soak runs are user time). Stopped at 02:57Z; nothing from it is cited.
