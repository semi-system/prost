#!/bin/sh
# Run after cargo test has built the native proxy integration test binary.
set -eu
cd "$(dirname "$0")/.."
docker run --rm --memory=96m --memory-swap=96m \
    -v "$(pwd):/work:ro" -v prost-target:/work/target:ro \
    -w /work prost-dev sh -ec '
    test_binary=$(find target/debug/deps -maxdepth 1 -name "proxy-*" -type f -executable -printf "%T@ %p\n" | sort -nr | head -n 1 | cut -d " " -f 2-)
    test -n "$test_binary"
    exec "$test_binary" static_files::static_ram_cgroup_limit_rejects_before_allocation --ignored --exact --nocapture
    '
