# Stopped in E1: the guard counted the harness's own docker calls as other work

This run (ae7a455) measured at full speed when it measured (row worker ~121K, Arrow worker ~147K, tuned sink ~197K wall rows/s), but in 29 minutes it discarded 15 attempts: the harness's own short-lived docker CLI calls (several per second during a case) finish between samples and were counted as other host work against a one-core limit. Fixed in 4d73c8e (reaped children count as the harness's CPU; limit 150%). Stopped at 02:51Z after 21 cases; no manifest was checkpointed; nothing from it is cited.
