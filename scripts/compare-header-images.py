"""Alternate two Prost images on identical header workloads to investigate regressions.

Run in WSL after prepare-advanced-bench.py. Uses the isolated advanced benchmark
project; do not run concurrently with another benchmark or build.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import statistics

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("advanced_bench", ROOT / "scripts/advanced-bench.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", default="prost:native")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--scale", type=float, default=5)
    parser.add_argument("--strip-rewrite", action="store_true", help="Use empty header filters in rewrite-only fixture routes for images predating rewrite support")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "test-results/header-image-control")
    args = parser.parse_args()
    if args.rounds < 1 or args.scale <= 0:
        parser.error("rounds and scale must be positive")
    config = ROOT / "test-results/advanced-fixtures/prost-native.toml"
    original = config.read_bytes()
    control = original.decode()
    replaced = 0
    if args.strip_rewrite:
        control, replaced = re.subn(r'name = "rewrite"\nconfig = [^\n]+\n', 'name = "headers"\nconfig = {}\n', control)
        if not replaced:
            parser.error("no rewrite fixture plugins found; regenerate fixtures first")
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    os.environ["PROST_ADVANCED_CONFIG"] = "prost-native.toml"
    images = {"before": args.before, "after": args.after}
    results = []
    try:
        config.write_text(control)
        (out / "prost-native.toml").write_text(control)
        bench.compose("up", "-d", "backend")
        bench.compose("stop", "prost", "nginx")
        bench.ready("backend")
        for round_number in range(args.rounds):
            labels = ["before", "after"] if round_number % 2 == 0 else ["after", "before"]
            cases = ["native_header_34", "native_full_34"]
            if round_number % 2:
                cases.reverse()
            for label in labels:
                os.environ["PROST_ADVANCED_IMAGE"] = images[label]
                bench.compose("up", "-d", "--no-deps", "--force-recreate", "--renew-anon-volumes", "prost")
                bench.ready("prost")
                for case in cases:
                    result = bench.measure("prost", case, out / f"{label}-{case}-round-{round_number+1}.json", args.scale)
                    result["label"] = label
                    results.append(result)
                bench.compose("stop", "prost")
        summary = {"method": "Alternating image order and case order; one proxy at a time, identical config, no nginx load or builds concurrently.",
                   "rounds": args.rounds, "scale": args.scale, "stripped_rewrite_plugins": replaced,
                   "config_sha256": hashlib.sha256(control.encode()).hexdigest(),
                   "source_note": "Image IDs identify the binaries; this test does not assert the current workspace source identifies an older image.",
                   "results": {}}
        for case in cases:
            row = summary["results"][case] = {}
            for label in images:
                group = [r for r in results if r["label"] == label and r["case"] == case]
                key = bench.container_name("prost")
                row[label] = {"image": images[label], "image_id": group[0]["images"][key],
                    "rps": statistics.median(r["rps"] for r in group), "rps_rounds": [r["rps"] for r in group],
                    "p99_ms": statistics.median(r["p99_ms"] for r in group),
                    "cpu_us": statistics.median(r["resources"][key]["cpu_us_per_request"] for r in group),
                    "working_set_mib": max(r["resources"][key]["working_set_peak_mib"] for r in group),
                    "requests": sum(r["requests"] for r in group), "failures": sum(r["failures"] for r in group)}
            rps_delta = (row["after"]["rps"] / row["before"]["rps"] - 1) * 100
            p99_delta = (row["after"]["p99_ms"] / row["before"]["p99_ms"] - 1) * 100
            row["gate"] = {"throughput_delta_percent": rps_delta, "p99_delta_percent": p99_delta,
                           "investigation_required": rps_delta < -5 or p99_delta > 10}
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2), flush=True)
        if any(r["failures"] for r in results):
            raise SystemExit(1)
    finally:
        config.write_bytes(original)
        bench.compose("stop")


if __name__ == "__main__":
    main()
