"""Generate a bounded streaming policy variant of the advanced native fixture."""
import argparse
from pathlib import Path
import re
import tomllib

ROOT = Path(__file__).resolve().parent.parent

def enable_limits(text):
    text, upstreams = re.subn(r"(?m)^\[upstreams\.[A-Za-z0-9_-]+\]$", lambda m: m[0] + "\nresponse_header_timeout_ms = 5000\nresponse_body_timeout_ms = 5000\nwrite_timeout_ms = 5000", text)
    text, listeners = re.subn(r"(?m)^\[\[listeners\]\]$", "[[listeners]]\nheader_read_timeout_ms = 5000\nclient_write_timeout_ms = 5000\nmax_request_body_bytes = 8388608\nrequest_body_timeout_ms = 5000", text)
    if not upstreams or not listeners:
        raise ValueError("expected advanced upstream/listener tables")
    parsed = tomllib.loads(text)
    if any(listener["protocol"] != "http" for listener in parsed["listeners"]):
        raise ValueError("benchmark policy requires HTTP listeners")
    return text

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=ROOT / "test-results/advanced-fixtures/prost-native.toml")
    parser.add_argument("--output", type=Path, default=ROOT / "test-results/advanced-fixtures/prost-limits.toml")
    args = parser.parse_args()
    args.output.write_text(enable_limits(args.input.read_text()))
    print(args.output)

if __name__ == "__main__":
    main()
