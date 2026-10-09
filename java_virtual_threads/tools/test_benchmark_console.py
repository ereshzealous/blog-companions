"""Evidence correctness and lifecycle checks; no performance thresholds."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmark_data import load_evidence
from benchmark_console import begin, finish, parse_prometheus, rates, export_html

ROOT = Path(__file__).resolve().parent.parent
SAVED = json.loads((ROOT/'benchmarks/results/jmh-results.json').read_text())


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def test_filtered_new_values_drive_chart_and_do_not_load_other_runs(self):
        entry = copy.deepcopy(next(e for e in SAVED if e['benchmark'].endswith('.scopedValue_reusedThread')))
        entry['primaryMetric']['rawData'] = [[9, 11]]
        (self.directory/'results.json').write_text(json.dumps([entry]))
        data = load_evidence(self.directory)
        self.assertEqual(data['configurations'], 1)
        self.assertEqual(len(data['panels']), 1)
        self.assertEqual(len(data['panels'][0]['rows']), 1)
        self.assertEqual(data['panels'][0]['charts'][0]['series'][0]['value'], 10)
        self.assertIn('10.0', data['panels'][0]['rows'][0][2])

    def test_jsonl_and_json_produce_the_same_results(self):
        path = self.directory/'results.json'; path.write_text(json.dumps(SAVED))
        expected = load_evidence(self.directory)['panels']; path.unlink()
        (self.directory/'results.jsonl').write_text('\n'.join(json.dumps(e) for e in SAVED))
        self.assertEqual(load_evidence(self.directory)['panels'], expected)

    def test_offline_export_has_no_dependency_on_article_assets(self):
        (self.directory/'results.json').write_text(json.dumps([SAVED[0]]))
        data = load_evidence(self.directory)
        output = self.directory/'report.html'
        with patch('benchmark_console.ROOT', self.directory):
            export_html(data, output)
        report = output.read_text()
        self.assertIn('id="saved-report"', report)
        self.assertIn(SAVED[0]['benchmark'], report)
        self.assertNotIn('<link rel="stylesheet"', report)
        self.assertNotIn('<script src=', report)

    def test_auxiliary_episode_mean_is_not_jmh_event_total(self):
        (self.directory/'results.json').write_text(json.dumps(SAVED))
        surge = next(p for p in load_evidence(self.directory)['panels'] if p['id'] == '03')
        row = next(r for r in surge['rows'] if r[0] == 'Static 5,000')
        self.assertEqual(row[3], '21.0%')

    def test_duplicate_or_corrupt_evidence_fails(self):
        path = self.directory/'results.json'; path.write_text(json.dumps([SAVED[0], SAVED[0]]))
        with self.assertRaisesRegex(ValueError, 'Duplicate'): load_evidence(self.directory)
        path.write_text('broken json')
        with self.assertRaises(ValueError): load_evidence(self.directory)

    def test_inconsistent_csv_outcomes_fail(self):
        (self.directory/'surge-outcomes.csv').write_text('gate,bucket_ms,arrivals,bid_in_sla,late_bid,timed_out,shed\na,0,10,8,0,0,0\n')
        with self.assertRaisesRegex(ValueError, 'inconsistent'): load_evidence(self.directory)

    def test_failed_run_does_not_publish_partial_or_old_success(self):
        run = self.directory/'run'
        begin(run, 'test failure', 'false')
        (run/'results.json').write_text(json.dumps(SAVED))
        with patch('benchmark_console.export_html'):
            finish(run, failed=True)
        result = json.loads((run/'report.json').read_text())
        self.assertEqual(result['manifest']['status'], 'failed')
        self.assertEqual(result['panels'], [])
        with self.assertRaisesRegex(ValueError, 'finalized'): finish(run)
        with self.assertRaisesRegex(ValueError, 'nonempty'): begin(run, 'test', 'false')

    def test_live_jsonl_is_not_misread_as_jmh(self):
        (self.directory/'results.json').write_text(json.dumps([SAVED[0]]))
        (self.directory/'live-metrics.jsonl').write_text('{"epoch":100,"at":"now","bidder":null}\n')
        self.assertEqual(load_evidence(self.directory)['configurations'], 1)


class TelemetryTests(unittest.TestCase):
    def test_only_bid_endpoint_counts_and_heap_pools_are_aggregated(self):
        metrics = parse_prometheus('''http_server_requests_seconds_count{uri="/openrtb/2.6/bid",status="200"} 20
http_server_requests_seconds_count{uri="/openrtb/2.6/bid",status="204"} 3
http_server_requests_seconds_count{uri="/actuator/prometheus"} 900
jvm_memory_used_bytes{area="heap",id="G1 Eden"} 10
jvm_memory_used_bytes{area="heap",id="G1 Old"} 25
jvm_memory_used_bytes{area="nonheap",id="Metaspace"} 800
process_cpu_usage NaN
''')
        self.assertEqual(metrics['requests'], 23)
        self.assertEqual(metrics['heap_bytes'], 35)
        self.assertIsNone(metrics['cpu'])

    def test_interval_rates_and_restart_do_not_create_false_spikes(self):
        old = dict(epoch=10, bidder=dict(requests=20, duration_seconds=1, uptime=100, shed=2, gc_seconds=.1))
        new = dict(epoch=15, bidder=dict(requests=30, duration_seconds=1.2, uptime=105, shed=4, gc_seconds=.2))
        actual = rates(old, new)
        self.assertEqual(actual['rps'], 2)
        self.assertAlmostEqual(actual['mean_ms'], 20)
        new['bidder']['uptime'] = 1
        self.assertTrue(all(v is None for v in rates(old, new).values()))
        self.assertTrue(all(v is None for v in rates(None, new).values()))


if __name__ == '__main__': unittest.main()
