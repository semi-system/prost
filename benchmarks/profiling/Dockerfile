ARG PROST_IMAGE=prost:native
FROM ${PROST_IMAGE} AS binaries
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends heaptrack procps \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/prost /var/lib/prost /profiles
COPY --from=binaries /usr/local/bin/prost /usr/local/bin/prostctl /usr/local/bin/
COPY benchmarks/profiling/entrypoint.sh /usr/local/bin/prost-entrypoint
RUN chmod 755 /usr/local/bin/prost-entrypoint
ENTRYPOINT ["/usr/local/bin/prost-entrypoint"]
