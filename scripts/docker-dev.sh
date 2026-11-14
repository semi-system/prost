#!/bin/sh
# Run from Linux/WSL; build products and registry stay in Linux Docker volumes.
set -eu
cd "$(dirname "$0")/.."
if [ "$#" -eq 0 ]; then set -- cargo test --workspace --locked; fi
docker build -f Dockerfile.dev -t prost-dev .
exec docker run --rm \
    -v "$(pwd):/work" \
    -v prost-target:/work/target \
    -v prost-cargo:/usr/local/cargo/registry \
    -w /work -e CARGO_BUILD_JOBS=2 \
    -e RUST_TEST_THREADS="${RUST_TEST_THREADS:-4}" prost-dev "$@"
