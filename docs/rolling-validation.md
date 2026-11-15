# Kiểm chứng rolling và TLS sources — 2026-11-15

Chạy local trong Docker/WSL, Rust 1.93.1, Cargo.lock hiện tại. Image native cuối:

```text
prost:native
sha256:c4ea8ec3bdb19ebd2034e995ac2998bffa4d366cd4c504bd704799ad03e50908
```

| Kiểm tra | Kết quả |
|---|---|
| rustfmt, Clippy workspace/all-targets/all-features, warnings denied | Pass |
| `cargo test --workspace --locked` | 210 pass |
| `cargo test --workspace --all-features --locked` | 210 pass |
| Bốn integration test rolling và một control API test, lặp 10 vòng | 50 pass |
| Python parser contracts | 6 pass |
| Build release trong Docker | Pass |
| Native S3, presigned HTTP, file URI, rolling TLS, offline restart | Pass trên image cuối |
| DNS refresh, TLS upstream, conditional cache/stale | Pass trên image cuối |
| Cache inspection/purge, variant, compatible reload/restart | Pass trên image cuối |
| Demo native filters, routing, balancing, endpoint patch, TCP và persistence | Pass trên image cuối |
| Hai vòng × 10.000 HTTP request, 16 client, 20 reload/vòng | Pass, không lỗi/retry, trên image cuối |

Integration rolling giữ đồng thời HTTP/1 response, HTTP/2 gRPC frames/trailers
và TCP cũ qua commit; request mới nhận route/header/backend mới. Có kiểm tra
generation conflict khi hai apply đồng thời, bind failure giữ config/worker cũ,
snapshot cũ được thu hồi, lần reload kế tiếp chờ retiring generation và TCP bị
đóng ở drain deadline. WebSocket và remote certificate rotation cũng chạy với
rolling. Bộ đếm endpoint dùng atomic RMW khi hai generation có cùng worker slot;
test tranh chấp trên state dùng chung kiểm tra chính xác 160.000 sự kiện mỗi loại.

Barrier `reload.wait` được kiểm tra qua Core và Unix socket: timeout giữ nguyên
commit, không giữ coordinator trong lúc chờ, chặn generation chưa commit,
giới hạn tám waiter nhưng status/validate vẫn hoạt động, và shutdown hủy wait
ngay. CLI kiểm tra alias `reload`, timeout bounds và `workers wait`. S3 release
smoke kiểm tra `reload --wait` timeout vẫn commit, giữ TLS cũ, rồi chờ thành công
ở lần reload kế tiếp và qua lệnh `workers wait` riêng.

Bài tải ban đầu bắt được bốn connection bị đóng trước response khi rolling rơi
vào khoảng giữa accept và đọc request đầu tiên. Đã sửa để đọc request đầu tiên
trên socket đã accept trước khi graceful drain, chịu header/drain deadline.
Test hồi quy trì hoãn request đầu HTTP/1 và HTTP/2 tới sau commit, chạy cả với
header timeout bật và bằng 0. Hai vòng tải trên image cuối đều không gặp lại lỗi.

## Tài nguyên trong bài tải rolling

Mỗi vòng là daemon mới, giới hạn 2 CPU/256 MiB, hai I/O worker. Fixture dùng
Python HTTP/TCP backend riêng; 16 client mở connection mới cho từng request,
không retry. Header generation phải khớp backend trả body. Mỗi reload cũng giữ
một TCP session qua commit rồi đóng nó và chờ worker cũ thoát. Đủ 20 reload của
mỗi vòng diễn ra khi traffic còn chạy.

| Quan sát riêng process Prost | Vòng 1 | Vòng 2 |
|---|---:|---:|
| Request thành công | 10.000/10.000 | 10.000/10.000 |
| Lỗi/retry | 0/0 | 0/0 |
| Thời gian bài chạy | 25,07 s | 25,28 s |
| CPU time process | 4,30 s | 4,54 s |
| RSS ban đầu → đỉnh lấy mẫu → cuối | 12,20 → 17,46 → 17,44 MiB | 12,07 → 16,98 → 16,97 MiB |
| FD ban đầu → đỉnh lấy mẫu → cuối | 34 → 85 → 40 | 34 → 77 → 40 |
| Thread ban đầu → cuối | 9 → 13 | 9 → 15 |
| Retired worker generation / forced | 20 / 0 | 20 / 0 |
| Live snapshot / worker generation cuối | 1 / 1 | 1 / 1 |

RSS/FD đỉnh được lấy mẫu, không phải cực đại tuyệt đối. Thread gồm runtime và
blocking pool; số cuối không được suy diễn thành I/O worker bị rò. Test chặn
FD cuối vượt baseline +16 và thread cuối vượt baseline +8; đây là kiểm tra hữu
hạn qua 20 lần reload, không chứng minh không leak ở mọi workload. CPU time lấy
từ `/proc/1/stat`; không gồm backend/load generator/monitor. Monitor riêng cùng
network namespace đọc metrics loopback, không mở metrics ra host.

Đây là bài kiểm tra chức năng và tài nguyên khi reload, không dùng để so RPS
với nginx/HAProxy hoặc suy ra capacity production. Backend/load generator,
fresh connection và các thao tác quản trị đều ảnh hưởng wall time.

S3 smoke dùng bucket riêng trên RustFS, credential fixture dùng một lần; unsigned
GET bị 403. Prost tải object bằng SigV4, hỗ trợ key percent-encoded và versionId,
rotate sang certificate mới nhưng giữ TLS session đang chạy. Object thiếu làm
validate/apply lỗi và không tăng generation. Dừng S3 rồi restart vẫn dùng được
artifact đã commit. Fixture còn kiểm tra presigned GET và key từ file URI.
Đây là kiểm chứng S3-compatible local, chưa chạy với AWS account/IAM thật.

HTTP smoke mở connection mới sau rolling reload. Demo đợi admin/listener/backend
ready sau restart trước khi kiểm tra hành vi; không retry các assertion nghiệp vụ.

Lệnh kiểm tra chính:

```sh
sh scripts/docker-dev.sh cargo fmt --all -- --check
sh scripts/docker-dev.sh cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
sh scripts/docker-dev.sh cargo test --workspace --locked
sh scripts/docker-dev.sh cargo test --workspace --all-features --locked
python3 -m unittest discover -s scripts/tests
python3 scripts/s3-tls-smoke.py --image prost:native --output-dir test-results/s3-new-run
python3 scripts/dns-cache-smoke.py --image prost:native --output-dir test-results/dns-new-run
python3 scripts/cache-control-smoke.py --image prost:native --output-dir test-results/cache-new-run
python3 scripts/rolling-load-smoke.py --image prost:native --requests 10000 --clients 16 --reloads 20 --output-dir test-results/rolling-new-run
```

Cache smoke cần backend image/certificate fixture mô tả trong
[cache-control.md](cache-control.md). Mỗi output directory phải mới; fixture chỉ
dọn container/network có UUID do chính lần chạy tạo ra.

Raw local (được gitignore):
[full suite/build](../test-results/rolling-ops-checks-v3/status.json),
[release smoke summary](../test-results/rolling-ops-release-final/summary.json),
[S3](../test-results/rolling-ops-release-final/s3/summary.json),
[DNS/cache](../test-results/rolling-ops-release-final/dns-cache/summary.json),
[cache control](../test-results/rolling-ops-release-final/cache-control/summary.json),
[load 1](../test-results/rolling-ops-release-final/load-1/summary.json),
[load 2](../test-results/rolling-ops-release-final/load-2/summary.json),
[demo](../test-results/rolling-ops-release-final/demo.log).

Các phép kiểm trên xác nhận chức năng, không phải benchmark hoặc soak 24 giờ cho
image này. Worker rolling vẫn là thread/runtime trong cùng process; xem
[rolling-updates.md](rolling-updates.md) và [tls-sources.md](tls-sources.md).
