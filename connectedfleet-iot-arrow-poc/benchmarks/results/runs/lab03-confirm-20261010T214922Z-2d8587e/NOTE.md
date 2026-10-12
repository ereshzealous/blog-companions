# Aborted after six cases: host CPU load from a native process

This confirm-profile run (2d8587e) started after 180 s without foreign Docker containers, but a native process on
the same laptop (`target/release/t14`, not a container) was using about 930% CPU (load average 23). Every lane ran
at half its normal speed, including the official sink (`sink_tuned` 82–87K rows/s against 174–191K in the previous
run). It was interrupted at about 21:53Z, after six E1 cases (the last at 21:52:18Z); no manifest was checkpointed, and nothing from it is used.

The launcher's idle check now also waits for native host processes to be quiet, and the host-load log samples them.
