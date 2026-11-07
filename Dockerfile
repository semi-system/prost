FROM rust:1.93-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
ENV CARGO_BUILD_JOBS=2
RUN cargo build --workspace --locked

FROM debian:bookworm-slim AS runtime
RUN groupadd --gid 10001 prost && useradd --uid 10001 --gid prost --no-create-home prost \
    && mkdir -p /run/prost /var/lib/prost /etc/prost \
    && chown -R prost:prost /run/prost /var/lib/prost /etc/prost
COPY --from=build /build/target/debug/prost /build/target/debug/prostctl /usr/local/bin/
COPY examples/prost.toml /etc/prost/seed.toml
COPY scripts/entrypoint.sh /usr/local/bin/prost-entrypoint
RUN chmod 755 /usr/local/bin/prost-entrypoint
USER prost
VOLUME ["/var/lib/prost"]
EXPOSE 8080 9000
ENTRYPOINT ["/usr/local/bin/prost-entrypoint"]
