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
from benchmark_cpu import partitions

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("advanced_bench", ROOT / "scripts/advanced-bench.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", default="prost:native")
    parser.add_argument("--before-config", type=Path, help="Optional config for the before label")
    parser.add_argument("--after-config", type=Path, help="Optional config for the after label; also allows same-image policy overhead comparisons")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--cases", nargs="+", choices=bench.CASES, default=["native_header_34", "native_full_34"])
    parser.add_argument("--duration", type=int, default=0, help="Timed h2load seconds per case; completed requests count, unfinished deadline streams reported separately")
    parser.add_argument("--rps-per-client", type=int, default=0, help="Optional fixed offered rate per connection; default is saturated closed-loop load")
    parser.add_argument("--scale", type=float, default=5)
    parser.add_argument("--strip-rewrite", action="store_true", help="Use empty header filters in rewrite-only fixture routes for images predating rewrite support")
    parser.add_argument("--pin-cpus", action="store_true", help="Separate client/proxy/backend onto 6 distinct guest physical cores")
    parser.add_argument("--capture-access-log", action="store_true", help="After each timed interval, flush an enabled access log and save cumulative writer counters")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "test-results/header-image-control")
    args = parser.parse_args()
    if args.rounds < 1 or args.scale <= 0 or args.duration < 0 or args.rps_per_client < 0:
        parser.error("rounds and scale must be positive")
    config = ROOT / "test-results/advanced-fixtures/prost-native.toml"
    original = config.read_bytes()
    control = original.decode()
    replaced = 0
    if args.strip_rewrite:
        control, replaced = re.subn(r'name = "rewrite"\nconfig = [^\n]+\n', 'name = "headers"\nconfig = {}\n', control)
        if not replaced:
            parser.error("no rewrite fixture plugins found; regenerate fixtures first")
    configs = {"before": args.before_config.read_text() if args.before_config else control,
               "after": args.after_config.read_text() if args.after_config else control}
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    if (out / "summary.json").exists() or list(out.glob("*-round-*.json")):
        parser.error("output directory already contains measurements; choose a fresh directory")
    containers_at_start = bench.run("docker", "ps", "--format", "{{.Names}}", capture_output=True, text=True).stdout.splitlines()
    affinity = partitions([2]) if args.pin_cpus else None
    if affinity:
        override = out.resolve() / "pinning.yaml"
        override.write_text("services:\n  prost:\n    cpuset: " + json.dumps(affinity["proxy"]) + "\n  backend:\n    cpuset: " + json.dumps(affinity["backends"][0]) + "\n")
        bench.COMPOSE += ["-f", str(override)]
        original_command = bench.command
        bench.command = lambda *a, **kw: ["taskset", "-c", affinity["client"], *original_command(*a, **kw)]
    if args.rps_per_client:
        unpaced_command = bench.command
        def paced_command(*a, **kw):
            command = unpaced_command(*a, **kw)
            command.insert(-1, f"--rps={args.rps_per_client}")
            return command
        bench.command = paced_command
    os.environ["PROST_ADVANCED_CONFIG"] = "prost-native.toml"
    images = {"before": args.before, "after": args.after}
    results = []
    try:
        config.write_text(configs["before"])
        (out / "prost-native.toml").write_text(configs["before"])
        (out / "prost-after.toml").write_text(configs["after"])
        bench.compose("up", "-d", "backend")
        bench.compose("stop", "prost", "nginx")
        bench.ready("backend")
        for round_number in range(args.rounds):
            labels = ["before", "after"] if round_number % 2 == 0 else ["after", "before"]
            cases = list(args.cases)
            if round_number % 2:
                cases.reverse()
            for label in labels:
                config.write_text(configs[label])
                os.environ["PROST_ADVANCED_IMAGE"] = images[label]
                bench.compose("up", "-d", "--no-deps", "--force-recreate", "--renew-anon-volumes", "prost")
                bench.ready("prost")
                for case in cases:
                    result = bench.measure("prost", case, out / f"{label}-{case}-round-{round_number+1}.json", args.scale, duration=args.duration)
                    result["label"] = label
                    result["round"] = round_number+1
                    if args.capture_access_log:
                        status = json.loads(bench.run("docker", "exec", bench.container_name("prost"), "prostctl", "status", capture_output=True, text=True).stdout)["result"]
                        if status.get("access_log") is not None:
                            bench.run("docker", "exec", bench.container_name("prost"), "prostctl", "access-log", "flush", "--if-generation", str(status["generation"]), capture_output=True, text=True)
                            status = json.loads(bench.run("docker", "exec", bench.container_name("prost"), "prostctl", "status", capture_output=True, text=True).stdout)["result"]
                        result["access_log_cumulative"] = status.get("access_log")
                    (out / f"{label}-{case}-round-{round_number+1}.json").write_text(json.dumps(result, indent=2) + "\n")
                    results.append(result)
                bench.compose("stop", "prost")
        summary = {"method": "Alternating image order and case order; one proxy at a time, identical config, no nginx load or builds concurrently.",
                   "rounds": args.rounds, "scale": args.scale, "duration_seconds":args.duration,"stripped_rewrite_plugins": replaced,"cpu_affinity":affinity,
                   "load_mode": "fixed offered rate" if args.rps_per_client else "saturated closed-loop",
                   "rps_per_client": args.rps_per_client,
                   "config_sha256": hashlib.sha256(configs["before"].encode()).hexdigest(),
                   "config_sha256_by_label": {label: hashlib.sha256(value.encode()).hexdigest() for label, value in configs.items()},
                   "containers_at_start": containers_at_start,
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
                    "p99_ms_rounds": [r["p99_ms"] for r in group],
                    "cpu_us": statistics.median(r["resources"][key]["cpu_us_per_request"] for r in group),
                    "cpu_us_rounds": [r["resources"][key]["cpu_us_per_request"] for r in group],
                    "throttled_ms_rounds": [r["resources"][key]["cpu_throttled_usec"]/1000 for r in group],
                    "worker_run_ms_rounds": [sum(v["run_ns"] for v in r["thread_cpu_ticks"].values() if v["name"].startswith("prost-io-"))/1e6 for r in group],
                    "worker_wait_ms_rounds": [sum(v["wait_ns"] for v in r["thread_cpu_ticks"].values() if v["name"].startswith("prost-io-"))/1e6 for r in group],
                    "working_set_mib": max(r["resources"][key]["working_set_peak_mib"] for r in group),
                    "requests": sum(r["requests"] for r in group), "unfinished_at_deadline":sum(r["unfinished_at_deadline"] for r in group), "failures": sum(r["failures"] for r in group),
                    "worker_cpu_ticks": [[v["ticks"] for v in r["thread_cpu_ticks"].values() if v["name"].startswith("prost-io-")] for r in group]}
                if args.capture_access_log:
                    row[label]["access_log_cumulative_by_round"] = [r["access_log_cumulative"] for r in group]
            rps_delta = (row["after"]["rps"] / row["before"]["rps"] - 1) * 100
            p99_delta = (row["after"]["p99_ms"] / row["before"]["p99_ms"] - 1) * 100
            row["gate"] = {"throughput_delta_percent": rps_delta, "p99_delta_percent": p99_delta,
                           "investigation_required": rps_delta < -5 or p99_delta > 10}
            row["same_binary_and_config"] = row["before"]["image_id"] == row["after"]["image_id"] and configs["before"] == configs["after"]
            row["gate"]["applicable_to_code_change"] = not row["same_binary_and_config"]
            if row["same_binary_and_config"]:
                row["calibration"] = {"note": "A/A control: differences measure environmental variation, not a code change.",
                                      "outside_noise_budget": abs(rps_delta) > 5 or abs(p99_delta) > 10}
                row["gate"]["investigation_required"] = row["calibration"]["outside_noise_budget"]
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2), flush=True)
        if any(r["failures"] for r in results):
            raise SystemExit(1)
    finally:
        config.write_bytes(original)
        bench.compose("stop")


if __name__ == "__main__":
    main()
