# Stopped in E1: a checksum query ran out of ClickHouse memory

Case E1-0012 failed: the de-duplicating content hash (GROUP BY event_id over 1M rows, ~1 GiB) hit ClickHouse's 2.25 GiB server limit (D-033) while merges were running. Drain runs load into emptied tables, so rows are unique; measure_table now de-duplicates only when physical != logical rows (as E3 already did). Stopped at 03:08Z after 12 cases; nothing from it is cited.
