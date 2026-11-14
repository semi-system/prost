"""Alternate uninstrumented memory profiles; run alone in WSL after building both images."""
import argparse
import json
from pathlib import Path
import statistics
import subprocess

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", default="prost:metrics-control")
    parser.add_argument("--after", default="prost:native")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--clients", type=int, default=128)
    parser.add_argument("--streams", type=int, default=32)
    parser.add_argument("--include-buffer-control", action="store_true")
    parser.add_argument("--include-limits-control", action="store_true", help="Also profile the after image with HTTP body limits and granular timeouts enabled")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "test-results/memory-comparison")
    args = parser.parse_args()
    if args.rounds < 1: parser.error("rounds must be positive")
    out = args.output_dir.resolve(); out.mkdir(parents=True, exist_ok=True)
    groups = {"before": args.before, "after": args.after}
    if args.include_buffer_control: groups["after_16k"] = args.after
    if args.include_limits_control: groups["after_limits"] = args.after
    results = {name: [] for name in groups}
    labels = list(groups)
    for n in range(args.rounds):
        order = labels[n % len(labels):] + labels[:n % len(labels)]
        for name in order:
            directory = out / f"{name}-{n+1}"
            command = ["python3", str(ROOT / "scripts/profile-memory.py"), "--image", groups[name],
                       "--requests", str(args.requests), "--clients", str(args.clients), "--streams", str(args.streams),
                       "--output-dir", str(directory)]
            if name == "after_16k": command += ["--tls-buffer-limit", "16384"]
            if name == "after_limits": command += ["--http-limits"]
            subprocess.run(command, check=True)
            results[name].append(json.loads((directory / "summary.json").read_text()))
    summary = {"rounds": args.rounds, "requests_per_profile": args.requests,
               "method": "Uninstrumented images, alternating order, fresh process, same stages; metrics scraped at each stage. No allocator environment overrides.",
               "results": {}}
    for name, group in results.items():
        stages = {}
        for i, stage in enumerate(group[0]["stages"]):
            rows = [g["stages"][i] for g in group]
            stages[stage["stage"]] = {
                "rss_mib": statistics.median(r["process"]["Rss"] / 1048576 for r in rows),
                "rss_rounds": [r["process"]["Rss"] / 1048576 for r in rows],
                "anonymous_mib": statistics.median(r["process"]["Anonymous"] / 1048576 for r in rows),
                "working_set_mib": statistics.median(r["cgroup"]["working_set_bytes"] / 1048576 for r in rows),
                "fds": statistics.median(r["fds"] for r in rows),
                "live_generations": [r["status"]["live_generations"] for r in rows],
            }
        summary["results"][name] = {"image": groups[name], "image_id": group[0]["metadata"]["image_id"], "http_limits": name == "after_limits", "stages": stages}
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__": main()
