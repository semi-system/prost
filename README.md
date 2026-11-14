# Prost

DNS backend có TTL/negative TTL và cache ETag/Last-Modified, stale-on-error:
[docs/dns-cache.md](docs/dns-cache.md).

Quản trị RAM cache: status/entries/key/purge qua Unix socket, chặn fill cũ ghi lại
sau purge: [docs/cache-control.md](docs/cache-control.md).

Routing HTTP theo method/header và ví dụ SNI default backend:
[docs/http-routing.md](docs/http-routing.md).

TLS passthrough chọn backend theo SNI, exact/wildcard và hot update:
[docs/passthrough-routing.md](docs/passthrough-routing.md).
Các phần chức năng còn lại: [docs/functional-roadmap.md](docs/functional-roadmap.md).

Runtime API/CLI liệt kê, xem và xóa state native rate limit/connection window:
[docs/rate-tables.md](docs/rate-tables.md).

Native Host ACL, HTTP/1 keepalive idle, TCP dial retry và connection window có
giới hạn tài nguyên: [docs/edge-policies.md](docs/edge-policies.md).
Ví dụ cấu hình chung: [examples/edge.toml](examples/edge.toml).

Kiểm chứng H2 p99, A/A noise và soak đúng image:
[docs/h2-validation.md](docs/h2-validation.md).

Trạng thái: dùng cho **MVP/pilot có giám sát**; chưa xác nhận cho production quan
trọng. Bằng chứng kiểm thử và điều kiện còn thiếu:
[docs/mvp-bundle-results.md](docs/mvp-bundle-results.md).

Gói native mở rộng (compression/cache/retry, mTLS/PROXY/passthrough,
timeout H2, metrics plugin/pool và soak local): [docs/mvp-bundle.md](docs/mvp-bundle.md).

Access log native JSONL, rotate qua Runtime API và metrics kết nối upstream thực tế:
[docs/access-log.md](docs/access-log.md).

GC artifact qua Runtime API và kiểm thử crash/đầy đĩa:
[docs/artifact-gc.md](docs/artifact-gc.md).

Streaming body limits, granular HTTP timeouts và ví dụ cấu hình:
[docs/http-limits.md](docs/http-limits.md).
Kết quả đo: [docs/http-limits-benchmark.md](docs/http-limits-benchmark.md).

MVP reverse proxy L4/L7 bằng Rust cho Linux. Cập nhật config bằng **Unix socket +
`prostctl`**, không polling, không watcher, không restart worker.

Đã triển khai TCP relay, TLS hai phía, HTTP/1.1–HTTP/2, HTTPS, WebSocket,
gRPC streaming/trailers, weighted round-robin/least-conn/consistent hash, TCP/TLS/HTTP health check, cấu hình
flow control HTTP/2 và bộ filter native: header/trailer, expires, map/điều kiện,
ACL và rate limit toàn cục/theo IP/header. WASM là feature tùy chọn, mặc định tắt.
Native rewrite path/query và redirect có điều kiện: [docs/rewrite.md](docs/rewrite.md).
So sánh rewrite với nginx và kiểm tra regression:
[docs/rewrite-benchmark.md](docs/rewrite-benchmark.md).
TLS hỗ trợ nhiều certificate theo SNI, exact/wildcard và hot rotate:
[docs/tls-sni.md](docs/tls-sni.md).
Cấu hình, vòng đời active load và affinity: [docs/load-balancing.md](docs/load-balancing.md).
Backend dự phòng và giới hạn request/stream/session trên từng endpoint:
[docs/upstream-capacity.md](docs/upstream-capacity.md).
Đo tải primary đầy và chuyển sang backup:
[docs/upstream-capacity-benchmark.md](docs/upstream-capacity-benchmark.md).
Passive health có ngưỡng lỗi, cửa sổ trượt, cooldown và HTTP status tùy chọn:
[docs/passive-health.md](docs/passive-health.md).
Đo tải healthy khi bật policy và đối chứng regression:
[docs/passive-health-benchmark.md](docs/passive-health-benchmark.md).
So sánh thuật toán với nginx trên bốn backend: [docs/load-balancing-benchmark.md](docs/load-balancing-benchmark.md).
Đây là MVP để chạy thử; soak dài và
đánh giá production vẫn còn cần thực hiện.

## Build và test bằng Docker trong WSL

GitHub Actions build image release bằng
[`abxst/actions/strict@v5`](https://github.com/abxst/actions/blob/v5/strict/action.yml)
sau khi job test thành công. PR, nhánh phụ và chạy thủ công chỉ build/scan;
push lên nhánh mặc định publish `ghcr.io/semi-system/prost:latest` và
`ghcr.io/semi-system/prost:sha-<full-commit-sha>`. Dùng `GITHUB_TOKEN` với quyền
`packages: write`, không cần PAT riêng. Image là Linux amd64, WASM tắt mặc định.
Semgrep/Trivy giữ gate strict, kể cả CVE chưa có bản vá; findings sẽ chặn publish.
Report được lưu trong artifact `image-security-reports` của workflow.

Cần Docker Engine chạy trong WSL Ubuntu và Docker Compose v2 để chạy demo.
Không cần cài Rust trên Windows.

Từ PowerShell, trong thư mục dự án:

```powershell
.\build.cmd test
.\build.cmd build
.\build.cmd fmt
.\build.cmd lint
```

Hoặc từ WSL:

```sh
sudo sh scripts/docker-dev.sh
sudo sh scripts/docker-dev.sh cargo build --workspace --locked
```

Registry và `target` ở Docker named volumes trên filesystem Linux, không build
vào `/mnt/d`. `Dockerfile.dev` cung cấp Rust, rustfmt và clippy.
Helper PowerShell/WSL và CI mặc định chạy test với `RUST_TEST_THREADS=4`;
có thể đặt biến môi trường này để đổi mức song song.
`build.cmd` gọi PowerShell với execution policy chỉ áp dụng cho process hiện tại,
không thay chính sách hệ thống. `Dockerfile` mặc định build **release** với LTO;
muốn image debug dùng `docker build --build-arg CARGO_PROFILE=dev -t prost:debug .`.

## Demo HTTP và TCP

Chạy từ WSL tại thư mục dự án:

```sh
sudo docker compose up -d --build
curl http://127.0.0.1:8080/
# {"backend": "blue", "path": "/"}

sudo docker compose exec prost prostctl status
sudo docker compose exec prost prostctl validate --file /examples/green.toml
sudo docker compose exec prost prostctl apply --file /examples/green.toml
curl http://127.0.0.1:8080/
# {"backend": "green", "path": "/"}

sudo docker compose restart prost
curl http://127.0.0.1:8080/
# vẫn là green: config đã được lưu vào volume
```

TCP echo ở `127.0.0.1:9000`; có thể thử bằng `nc 127.0.0.1 9000`.
Kết nối TCP/WebSocket đã mở vẫn tiếp tục sau apply. HTTP request mới, kể cả trên
keep-alive, dùng config mới.

```sh
sudo docker compose exec prost prostctl upstream disable web/green
# HTTP trả 503 vì không còn endpoint khả dụng
sudo docker compose exec prost prostctl upstream enable web/green
sudo docker compose exec prost prostctl config export
sudo docker compose down
```

Chạy smoke test tự động cho demo đang hoạt động:

```sh
sudo python3 scripts/smoke.py
```

`down` giữ state. Để reset demo có chủ đích, `docker compose down -v` xóa volume
state của demo. Khi đổi lại cấu hình blue, dùng `apply --file /examples/prost.toml`.

## Chạy binary độc lập

```sh
prost check --config /etc/prost/prost.toml
prost run --config /etc/prost/prost.toml
prostctl apply --file candidate.toml --if-generation 12
```

Thư mục config phải ghi được: daemon thay file bằng atomic rename và tạo lock
file cạnh nó. Trong container, file được quản lý nằm ở
`/var/lib/prost/prost.toml`; `/etc/prost/seed.toml` chỉ dùng khởi tạo lần đầu.
Không bind-mount riêng một file config làm state vì atomic rename cần thư mục.

Sửa TOML trực tiếp không đổi config đang chạy. CLI tự lấy generation nếu không
truyền `--if-generation`; nếu có cập nhật đồng thời, lệnh trả conflict. Sau lỗi
mất kết nối, kiểm tra `status` trước khi gửi lại. Certificate/WASM được sao chép
vào kho artifact theo hash; thay file nguồn cần một lần apply mới.

## Config

Xem [config demo](examples/prost.toml), [chuyển sang green](examples/green.toml)
và [IMAPS → IMAP](examples/imaps.toml).

HTTP route nằm trong listener:

```toml
[[listeners.routes]]
host = "api.example.com"
path_prefix = "/api"
priority = 10
upstream = "web"
```

Với gRPC upstream, thêm `http2 = true` vào `[upstreams.web]`.
Với HTTPS, thêm block `listeners.tls` như ví dụ IMAPS. Muốn TLS tới upstream,
thêm `upstreams.<name>.tls` với `server_name` và CA tùy chọn.

Pool HTTP upstream mặc định giữ tối đa 128 connection idle/host, timeout 30 giây.
Có thể thay đổi khi apply config:

```toml
[upstreams.web]
pool_max_idle_per_host = 128
pool_idle_timeout_ms = 30000
```

Đây là giới hạn connection **idle**, không phải active. Giảm giá trị nếu có
nhiều upstream và cần tiết kiệm file descriptor/RAM.

Mặc định, mỗi I/O worker có runtime đơn luồng và pool riêng; tổng idle budget
vẫn được chia trong giới hạn trên. Linux dùng `SO_REUSEPORT` để phân phối
connection đến worker. Có thể cấu hình lúc startup:

```toml
[runtime]
worker_local = true
io_workers = 2
```

`io_workers = 0` tự chọn theo runtime gọi (tối đa 8); giá trị explicit tối đa 32.
`worker_local = false` dùng lại runtime chia sẻ. Hai tùy chọn này cần restart;
route/upstream/certificate/plugin vẫn apply khi đang chạy. Histogram latency
được gộp theo worker mỗi 64 mẫu hoặc 250 ms và flush khi shutdown; không bỏ mẫu.

Plugin inline, theo thứ tự khai báo:

```toml
[[listeners.plugins]]
name = "wasm"
module = "/examples/add-header.wat"
config = { label = "demo" }
```

Build mặc định có `headers`, `rewrite`, `ip_acl`, `rate_limit`, `connection_limit`.
Ví dụ WASM trên cần `--build-arg CARGO_FEATURES=prost-cli/wasm` khi build Docker,
hoặc `cargo build --release --features prost-cli/wasm`.
Filter tùy biến đăng ký qua API Rust; thêm code native cần build lại.

Bộ header đầy đủ: [docs/native-headers.md](docs/native-headers.md), cấu hình mẫu
[examples/native-headers.toml](examples/native-headers.toml).
Map/điều kiện và rate limit theo API key có mẫu chạy được trong Compose:
[examples/native-policies.toml](examples/native-policies.toml).
Đo riêng map/điều kiện và limiter native với nginx:
[docs/native-policies-benchmark.md](docs/native-policies-benchmark.md).

## API và tài liệu

Frame: length 4 byte big-endian + JSON, tối đa 4 MiB. Một request/connection:

```json
{"version":1,"id":"example","method":"status"}
```

Methods: `status`, `validate`, `apply`, `upstream.set_enabled`, `config.export`.
Socket mặc định `/run/prost/admin.sock`, quyền `0600`. Không có admin TCP port.
Export chứa đường dẫn artifact, không chứa nội dung private key.
Metrics demo chỉ bind loopback trong container ở `127.0.0.1:9090/metrics`.
Metrics endpoint/health/capacity, TLS, transaction, generation và process:
[docs/observability.md](docs/observability.md).
Đo heap/RSS, idle connections và thu hồi sau cancellation/apply:
[docs/memory-profiling.md](docs/memory-profiling.md).
Kết quả đo và đánh đổi RAM/throughput:
[docs/observability-memory-benchmark.md](docs/observability-memory-benchmark.md).

Chi tiết transaction, WASM ABI, giới hạn hiện tại và phần tham khảo Broom:
[docs/architecture.md](docs/architecture.md).

Kết quả đo 10.000 request, 100 client đồng thời trên MVP debug:
[docs/load-test-10000.md](docs/load-test-10000.md). Script đo lại nằm ở
`scripts/loadtest.py`; CPU/RAM proxy được đo riêng bằng cgroup v2.

Đối chiếu cùng môi trường với nginx và Apache2, có cả Prost release và hai bộ
phát tải: [docs/proxy-comparison-10000.md](docs/proxy-comparison-10000.md).

Phân tích cơ chế nginx, đối chiếu code Prost và đo thêm mức tái sử dụng TCP:
[docs/nginx-performance-research.md](docs/nginx-performance-research.md).

Kết quả sau tối ưu, gồm bản baseline release và nginx đo lại cùng lượt:
[docs/performance-optimization.md](docs/performance-optimization.md).

Đối chiếu PGO với nginx cùng ngân sách pool, giới hạn kết luận và lệnh tái hiện:
[docs/pgo-performance.md](docs/pgo-performance.md).

Health check ứng dụng, ngưỡng lỗi/phục hồi và HTTP/2 settings:
[docs/advanced-features.md](docs/advanced-features.md).

Bộ native header/ACL/rate/connection filters và ví dụ config inline:
[docs/native-filters.md](docs/native-filters.md).
