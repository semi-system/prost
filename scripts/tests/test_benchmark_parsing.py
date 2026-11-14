import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(SCRIPTS))
spec = importlib.util.spec_from_file_location("advanced_bench", SCRIPTS/"advanced-bench.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)
symbols_spec = importlib.util.spec_from_file_location("symbolize_http_cpu", SCRIPTS/"symbolize-http-cpu.py")
symbols = importlib.util.module_from_spec(symbols_spec)
symbols_spec.loader.exec_module(symbols)


class Parsing(unittest.TestCase):
    def test_exiting_thread_does_not_publish_partial_scheduling_sample(self):
        task = MagicMock()
        task.name = "123"
        files = {"stat": "123 (prost-io-0) " + " ".join(["0"]*20), "comm": "prost-io-0\n"}
        def read_file(name):
            file = MagicMock()
            if name == "schedstat":
                file.read_text.side_effect = FileNotFoundError()
            else:
                file.read_text.return_value = files[name]
            return file
        task.__truediv__.side_effect = read_file
        with patch.object(bench, "Path") as path:
            path.return_value.iterdir.return_value = [task]
            self.assertEqual(bench.thread_cpu(1), {})

    def test_profile_preserves_named_and_raw_symbols_with_optional_ipc(self):
        raw = '''# Overhead Shared Object Symbol
    19.30%  prost       [.] _aesni_ctr32_ghash_6x       -      -
     7.12%  libc.so.6   [.] 0x0000000000152d8a          -      -
     0.84%  prost       [.] prost_core::RunningProxy::handle_http::{{closure}}
     0.70%  prost       [.] <a::B as c::D>::poll        1.2    -
     0.01%  [kernel]    [k] ignored
'''
        self.assertEqual(list(symbols.leaf_rows(raw)), [
            (19.3, "prost", "_aesni_ctr32_ghash_6x"),
            (7.12, "libc.so.6", "0x0000000000152d8a"),
            (0.84, "prost", "prost_core::RunningProxy::handle_http::{{closure}}"),
            (0.70, "prost", "<a::B as c::D>::poll")])

    def parse(self, *, timed=False, started=4, data=400, records=None):
        raw = (f"requests: 4 total, {started} started, 4 done, 4 succeeded, 0 failed, 0 errored, 0 timeout\n"
               f"100.0 req/s\ntraffic: ({data}) data\n")
        with tempfile.TemporaryDirectory() as directory:
            log=Path(directory)/"requests.tsv"
            log.write_text(records or "1 200 1000\n2 200 2000\n3 200 3000\n4 200 4000\n")
            return bench.parse(raw,4,100,log,timed=timed)

    def test_fixed_requires_exact_body_and_request_counts(self):
        self.assertEqual(self.parse()["failures"],0)
        self.assertGreater(self.parse(data=450)["failures"],0)
        self.assertGreater(self.parse(started=6)["failures"],0)

    def test_timed_accounts_completed_and_separate_deadline_tail(self):
        result=self.parse(timed=True,started=6,data=450)
        self.assertEqual(result["failures"],0)
        self.assertEqual(result["requests"],4)
        self.assertEqual(result["unfinished_at_deadline"],2)
        self.assertEqual(result["p99_ms"],4)

    def test_timed_rejects_bytes_outside_completed_and_started_bounds(self):
        for data in [399,601]:
            self.assertGreater(self.parse(timed=True,started=6,data=data)["failures"],0)

    def test_timed_does_not_hide_status_or_missing_latency_record(self):
        self.assertGreater(self.parse(timed=True,records="1 503 1000\n2 200 1000\n3 200 1000\n4 200 1000\n")["failures"],0)
        self.assertGreater(self.parse(timed=True,records="1 200 1000\n")["failures"],0)


if __name__ == "__main__": unittest.main()
