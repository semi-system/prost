# Benchmark native header và workload nặng — 2026-11-11

Mỗi proxy hoàn tất **408,000 request đo** trên 13 workload, 3 vòng. Lỗi: Prost 0, nginx 0.

Raw: `test-results/native-verified-final/`. Bảng dùng trung vị các vòng; RAM là đỉnh working set lấy mẫu lớn nhất.

## Cùng điều kiện

- CPU: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz; kernel `6.18.54.1-microsoft-standard-WSL2`; Docker trong WSL2.
- Mỗi proxy và backend: quota 2 CPU, giới hạn 256 MiB. Release portable, thin LTO, không PGO.
- Phiên bản: `prost-cli 0.1.0`, `nginx version: nginx/1.28.0`; driver `h2load nghttp2/1.68.0`.
- Cùng backend Go, payload, certificate, client concurrency và timeout; không access log hoặc response cache.
- nginx: 2 worker, reuseport, buffer streaming 64 KiB, body preread HTTP/2 1 MiB; pool HTTP/HTTPS/gRPC riêng, 128 idle tổng mỗi pool. Buffering và retry tắt.
- Prost: 2 I/O worker, HTTP/1 upstream buffer tối đa 64 KiB, pool idle có timer 30 giây. WASM tắt khi build.
- Phần lớn HTTP/2 dùng 32 connection × 4 stream; upload/delayed stream dùng 16 × 4. Thứ tự proxy và workload luân phiên.
- Warmup 256 request trước mỗi phép đo. Log request và file upload đặt trên filesystem Linux; không build/profile khi đo.
- Preflight kiểm tra protocol upstream, byte body, hash upload, header động/expires và native/gRPC trailers. Khi đo kiểm tra status 200, số response, số mẫu latency và tổng byte. Backend kiểm tra hash mỗi upload.

Expires/Cache-Control trong ca header động là header cho client. Driver không cache và cả hai proxy đều forward request tới backend.

## Throughput và latency

| Workload | Prost req/s | nginx req/s | Chênh lệch Prost | p99 Prost / nginx (ms) |
|---|---:|---:|---:|---:|
| HTTP/1 download 1 MiB | 2,608 | 2,901 | -10.1% | 26.71 / 24.47 |
| HTTPS/1 download 64 KiB | 12,815 | 10,436 | +22.8% | 11.98 / 13.32 |
| HTTPS/2 download 64 KiB | 12,304 | 9,969 | +23.4% | 14.72 / 15.86 |
| TLS hai phía, upstream HTTP/1 | 9,578 | 8,291 | +15.5% | 17.72 / 19.99 |
| HTTPS/2 upload 1 MiB + SHA-256 | 774 | 740 | +4.6% | 166.86 / 168.84 |
| HTTPS/2 stream 256 KiB | 1,469 | 1,510 | -2.7% | 44.04 / 43.56 |
| gRPC framed stream + trailers | 1,525 | 1,561 | -2.3% | 63.84 / 48.92 |
| 1.000 route, body 64 KiB | 11,781 | 9,664 | +21.9% | 14.43 / 17.39 |
| Header tĩnh, body 64 KiB | 12,485 | 10,649 | +17.2% | 13.49 / 16.16 |
| Header tĩnh, body 34 byte | 43,360 | 30,200 | +43.6% | 4.50 / 5.70 |
| ACL + rate limit + header | 39,814 | 31,597 | +26.0% | 5.08 / 6.51 |
| Header động + request ID + expires | 31,705 | 26,135 | +21.3% | 5.91 / 7.29 |
| Native trailer, body 64 KiB | 11,783 | 9,720 | +21.2% | 13.96 / 16.78 |

## CPU và bộ nhớ

| Workload | CPU µs/request Prost / nginx | Working set MiB Prost / nginx |
|---|---:|---:|
| HTTP/1 download 1 MiB | 660.41 / 557.99 | 66.27 / 55.81 |
| HTTPS/1 download 64 KiB | 142.12 / 182.07 | 55.89 / 42.20 |
| HTTPS/2 download 64 KiB | 159.70 / 180.94 | 40.00 / 27.09 |
| TLS hai phía, upstream HTTP/1 | 182.09 / 215.68 | 54.76 / 32.81 |
| HTTPS/2 upload 1 MiB + SHA-256 | 1,207.89 / 1,225.86 | 89.92 / 106.23 |
| HTTPS/2 stream 256 KiB | 547.22 / 748.31 | 57.37 / 29.63 |
| gRPC framed stream + trailers | 944.74 / 1,097.45 | 60.09 / 36.85 |
| 1.000 route, body 64 KiB | 153.85 / 185.97 | 61.15 / 43.09 |
| Header tĩnh, body 64 KiB | 147.10 / 175.30 | 61.19 / 42.78 |
| Header tĩnh, body 34 byte | 42.05 / 59.18 | 57.77 / 38.21 |
| ACL + rate limit + header | 43.13 / 61.13 | 58.14 / 37.96 |
| Header động + request ID + expires | 49.82 / 67.00 | 57.64 / 38.00 |
| Native trailer, body 64 KiB | 159.82 / 176.46 | 58.96 / 42.98 |

Prost dùng working set đỉnh cao hơn nginx ở 12/13 workload. Đây là số đo cgroup (`memory.current - inactive_file`), không phải heap/RSS riêng. Lấy mẫu mỗi 50 ms có thể bỏ lỡ spike. Mỗi proxy giữ cùng process qua nhiều workload trong một vòng, nên cache/buffer còn lại từ ca trước ảnh hưởng RAM.

## Client đọc chậm, cùng workload

| Proxy | Clients / thời gian | Working set đỉnh MiB | Kết quả |
|---|---:|---:|---|
| prost | 32 / 10s | 126.35 | Không lỗi, không OOM; 0 connection, 1 generation sau cleanup |
| nginx | 32 / 10s | 83.81 | Không lỗi, không OOM |

## Hot update khi stream đang chạy

| Proxy | Clients / thời gian | Working set đỉnh MiB | Kết quả |
|---|---:|---:|---|
| prost | 32 / 10s | 104.18 | Không lỗi, không OOM; 0 connection, 1 generation sau cleanup |
| nginx | 32 / 10s | 94.43 | Không lỗi, không OOM |

Prost disable/enable endpoint trong lúc stream cũ tiếp tục đọc; request mới lần lượt nhận 503/200. nginx chạy cùng slow-reader workload nhưng không đổi config, nên RAM của phép thử này không phải so sánh thao tác cập nhật tương đương.

## Phạm vi kết luận

- Đây là workload local trên máy dùng chung, không CPU pinning hoặc load generator riêng; không suy rộng thành mọi workload/production capacity.
- gRPC dùng fixture byte-framing và grpc-status, không có chi phí protobuf/business handler. Hai proxy đều xác minh TLS upstream và dùng keep-alive; kiến trúc multiplex/pool vẫn khác nhau.
- Direct-backend control nằm trong raw. Protocol của direct client có thể khác protocol upstream sau proxy, nên không dùng direct RPS làm trần tuyệt đối hoặc trừ để suy ra overhead.
- Các thư mục đo trước `native-verified-final` là exploratory; đã phát hiện và sửa ALPN cùng cấu hình keep-alive gRPC. Không dùng chúng để kết luận so sánh cuối.

Source SHA-256: `b37255504b1be2838da89a8d1bf5dc9ef7f8e17c66861446bae7ca36d662decd`. Image ID, limits, config hash và toàn bộ mẫu đo có trong JSON từng vòng.

Lệnh tái hiện: [benchmarks/README.md](../benchmarks/README.md). Hành vi và giới hạn tính năng: [native-headers.md](native-headers.md).
