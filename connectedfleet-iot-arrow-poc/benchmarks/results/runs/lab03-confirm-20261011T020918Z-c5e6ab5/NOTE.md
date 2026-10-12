# Stopped after a few E1 warm-ups: the guard re-waited 60 s before every case

This run (c5e6ab5) started on a quiet host at 02:09Z and its cases ran at normal speed, but the host guard waited a full 60 quiet seconds before every case, even when the host had been quiet throughout: about 220 timing cases would have added almost four hours. The guard now carries quiet time across cases (only the first case, or a case after other work, waits the full window). Stopped at 02:20Z after six cases; no manifest was checkpointed; nothing from it is cited.
