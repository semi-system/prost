"""Resolve leaf IPs when the WSL perf build lacks ELF/DWARF support.

Uses exact recorded image IDs and binutils in the existing Rust build image.
This recovers sampled function names, not inline frames or complete call stacks.
"""
import argparse
import bisect
from collections import defaultdict
import json
from pathlib import Path
import re
import struct
import subprocess
import tempfile


def leaf_rows(raw):
    """Accept both perf's resolved names and raw IPs, with optional IPC columns."""
    for line in raw.splitlines():
        match = re.match(r"^\s*([\d.]+)%\s+(\S+)\s+\[\.\]\s+(.+?)\s*$", line)
        if match:
            weight, dso, symbol = match.groups()
            symbol = re.split(r"\s{2,}", symbol, maxsplit=1)[0]
            yield float(weight), dso, symbol


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("directory", type=Path)
    args = p.parse_args()
    directory = args.directory.resolve()
    for data in sorted(directory.glob("*.perf.data")):
        index = data.name.split(".")[0]
        image = (directory / f"{index}.image.txt").read_text().splitlines()[1]
        raw = subprocess.run(["perf", "report", "-f", "--stdio", "--no-children", "--sort", "dso,symbol", "--percent-limit", "0", "--no-call-graph", "-i", str(data)], capture_output=True, text=True, check=True).stdout
        (directory / f"{index}.leaf.txt").write_text(raw)
        with tempfile.TemporaryDirectory(prefix="prost-symbols-") as temporary:
            binary = Path(temporary) / "prost"
            with binary.open("wb") as stream:
                subprocess.run(["docker", "run", "--rm", "--entrypoint", "/bin/cat", image, "/usr/local/bin/prost"], stdout=stream, check=True)
            symbols = subprocess.run(["docker", "run", "--rm", "-v", f"{temporary}:/symbols:ro", "rust:1.93-bookworm", "nm", "-n", "-S", "--defined-only", "--demangle=rust", "/symbols/prost"], capture_output=True, text=True, check=True).stdout
            entries = []
            for line in symbols.splitlines():
                fields = line.split(None, 3)
                if len(fields) == 4 and fields[2] in ("t", "T"):
                    start, size = int(fields[0], 16), int(fields[1], 16)
                    if size: entries.append((start, size, fields[3]))
            entries.sort()
            starts = [entry[0] for entry in entries]
            elf = binary.read_bytes()
            if elf[:6] != b"\x7fELF\x02\x01": raise ValueError("expected ELF64 little-endian")
            phoff = struct.unpack_from("<Q", elf, 32)[0]
            phsize, phnum = struct.unpack_from("<HH", elf, 54)
            segments = []
            for n in range(phnum):
                at = phoff + n * phsize
                kind, flags, offset, virtual, _, size = struct.unpack_from("<IIQQQQ", elf, at)
                if kind == 1 and flags & 1: segments.append((offset, virtual, size))
            totals = defaultdict(float)
            dsos = defaultdict(float)
            for weight, dso, symbol in leaf_rows(raw):
                dsos[dso] += weight
                if dso != "prost": continue
                if not re.fullmatch(r"0x[0-9a-f]+", symbol):
                    totals[symbol] += weight
                    continue
                ip = int(symbol, 16)
                for offset, virtual, size in segments:
                    if offset <= ip < offset + size: ip = ip - offset + virtual; break
                i = bisect.bisect_right(starts, ip) - 1
                name = entries[i][2] if i >= 0 and ip < entries[i][0] + entries[i][1] else "<unresolved>"
                totals[name] += weight
            report = {"image_id": image, "note": "Leaf function self percentages only; no DWARF/inline call stacks. Preserves perf-resolved names and resolves remaining raw Prost IPs. Percentages aggregate rounded perf rows.", "dso_percent": dict(dsos), "functions": sorted(totals.items(), key=lambda pair: -pair[1])}
            (directory / f"{index}.symbols.json").write_text(json.dumps(report, indent=2) + "\n")
            print(image)
            for name, percent in report["functions"][:15]: print(f"{percent:6.2f}% {name}")

if __name__ == "__main__": main()
