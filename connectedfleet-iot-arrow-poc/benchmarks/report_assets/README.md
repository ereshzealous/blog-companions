# Results page assets

`report.css` is the base page layout and `fonts.css` embeds Lilita One, Nunito, Comic Shanns and Cascadia Code as
WOFF2 data, so `benchmarks/results/benchmark-results.html` and each run's `report.html` work offline. The panel styles
live in `src/connectedfleet/bench/results_html.py`. Both files come from the author's earlier "Java at Scale" results
report, unchanged except that the generator overrides the accent colours.

Font licenses are in `licenses/`: Lilita One (SIL OFL), Nunito (SIL OFL), Comic Shanns (MIT), Cascadia Code (SIL OFL).
