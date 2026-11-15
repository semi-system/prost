import sys
from pathlib import Path
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mail_stress_load import parse


class MailStressParsing(unittest.TestCase):
    def test_timed_cutoff_is_distinct_from_http_failures_and_missing_records(self):
        raw = "finished in 2s, 1.50 req/s\nrequests: 3 total, 5 started, 3 done, 3 succeeded, 0 failed, 0 errored, 0 timeout"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"requests.tsv"
            path.write_text("0 200 123\n1 200 456\n2 200 789\n")
            result = parse(raw, path)
            self.assertEqual(result["failures"], 0)
            self.assertEqual(result["unfinished_at_deadline"], 2)
            self.assertEqual(result["p99_ms"], .8)
            path.write_text("0 200 123\n1 503 456\n2 200 789\n")
            self.assertEqual(parse(raw, path)["failures"], 1)
            path.write_text("0 200 123\n")
            self.assertEqual(parse(raw, path)["failures"], 2)

    def test_no_completed_requests_and_unparseable_output_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"requests.tsv"
            path.write_text("")
            raw = "finished in 2s, 0.00 req/s\nrequests: 0 total, 5 started, 0 done, 0 succeeded, 0 failed, 0 errored, 0 timeout"
            self.assertGreater(parse(raw, path)["failures"], 0)
            with self.assertRaises(ValueError):
                parse("unexpected driver output", path)
