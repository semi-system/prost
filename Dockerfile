FROM rust:1.93-bookworm AS toolchain
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl \
    && cargo install cargo-auditable --version 0.7.7 --locked
ENV CARGO_BUILD_JOBS=2
ENV CC_x86_64_unknown_linux_musl=musl-gcc

FROM toolchain AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
ARG CARGO_PROFILE=release
ARG CARGO_FEATURES=""
RUN cargo auditable build --workspace --locked --target x86_64-unknown-linux-musl --profile "$CARGO_PROFILE" --features "$CARGO_FEATURES" \
    && mkdir /out \
    && if [ "$CARGO_PROFILE" = dev ]; then \
         cp target/x86_64-unknown-linux-musl/debug/prost target/x86_64-unknown-linux-musl/debug/prostctl target/x86_64-unknown-linux-musl/debug/prost-entrypoint /out/; \
       else \
         cp "target/x86_64-unknown-linux-musl/$CARGO_PROFILE/prost" "target/x86_64-unknown-linux-musl/$CARGO_PROFILE/prostctl" "target/x86_64-unknown-linux-musl/$CARGO_PROFILE/prost-entrypoint" /out/; \
       fi \
    && mkdir -p /runtime/run/prost /runtime/var/lib/prost /runtime/etc/prost

FROM ghcr.io/semi-system/baseless-static@sha256:0818dee927a7fdd28e358e3337c891b878ebc58e9e20094d236ae3fe03a09f1b AS runtime
LABEL io.prost.runtime="baseless-static"
COPY --from=build --chown=10001:10001 /runtime/run/prost/ /run/prost/
COPY --from=build --chown=10001:10001 /runtime/var/lib/prost/ /var/lib/prost/
COPY --from=build --chown=10001:10001 /runtime/etc/prost/ /etc/prost/
COPY --from=build /out/prost /out/prostctl /usr/local/bin/
COPY --from=build /out/prost-entrypoint /usr/local/bin/prost-entrypoint
COPY examples/prost.toml /etc/prost/seed.toml
USER 10001:10001
ENV HOME=/var/lib/prost
WORKDIR /
VOLUME ["/var/lib/prost"]
EXPOSE 8080 9000
ENTRYPOINT ["/usr/local/bin/prost-entrypoint"]
