FROM rust:1.93-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
ENV CARGO_BUILD_JOBS=2
ARG CARGO_PROFILE=release
RUN cargo build --workspace --locked --profile "$CARGO_PROFILE" \
    && mkdir /out \
    && if [ "$CARGO_PROFILE" = dev ]; then \
         cp target/debug/prost target/debug/prostctl /out/; \
       else \
         cp "target/$CARGO_PROFILE/prost" "target/$CARGO_PROFILE/prostctl" /out/; \
       fi

FROM debian:bookworm-slim AS runtime
RUN groupadd --gid 10001 prost && useradd --uid 10001 --gid prost --no-create-home prost \
    && mkdir -p /run/prost /var/lib/prost /etc/prost \
    && chown -R prost:prost /run/prost /var/lib/prost /etc/prost
COPY --from=build /out/prost /out/prostctl /usr/local/bin/
COPY examples/prost.toml /etc/prost/seed.toml
COPY scripts/entrypoint.sh /usr/local/bin/prost-entrypoint
RUN chmod 755 /usr/local/bin/prost-entrypoint
USER prost
VOLUME ["/var/lib/prost"]
EXPOSE 8080 9000
ENTRYPOINT ["/usr/local/bin/prost-entrypoint"]
