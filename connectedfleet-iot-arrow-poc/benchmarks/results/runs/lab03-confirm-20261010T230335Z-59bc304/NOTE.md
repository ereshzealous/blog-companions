# Stopped during E1 warm-ups: the host guard measured load from a decaying average

This run (59bc304) was the first with the per-case host guard (D-032). The guard discarded and re-ran a warm-up three
times while other work ran on the laptop, which worked as designed. But it read `ps %cpu`, a decaying average over up
to a minute, so a job that started during a short case barely registered: a tuned-sink warm-up ran at 125K rows/s
(about 190K on a quiet host) without being flagged. The guard now measures CPU-time differences between samples
(`hostload.CpuDelta`). Stopped after the first warm-ups; no manifest was checkpointed; nothing from it is cited.
