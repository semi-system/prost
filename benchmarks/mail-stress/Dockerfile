FROM golang:1.25-bookworm AS build
WORKDIR /build
COPY backend.go .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /backend backend.go
FROM debian:bookworm-slim
COPY --from=build /backend /usr/local/bin/backend
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/backend"]
