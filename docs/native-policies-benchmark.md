# Benchmark native header và workload nặng — 2026-11-11

Mỗi proxy hoàn tất **360,000 request đo** trên 6 workload, 3 vòng. Lỗi: Prost 0, nginx 0.

Raw: `test-results/native-policies-final/`. Bảng dùng trung vị các vòng; RAM là đỉnh working set lấy mẫu lớn nhất.

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
| Header tĩnh, body 34 byte | 45,861 | 31,365 | +46.2% | 3.85 / 5.75 |
| Header động + request ID + expires | 34,393 | 27,115 | +26.8% | 4.74 / 6.75 |
| ACL + rate limit + header | 40,777 | 32,191 | +26.7% | 4.27 / 4.96 |
| Map exact + condition + expires | 36,286 | 29,819 | +21.7% | 4.67 / 6.47 |
| Map regex + condition + expires | 39,293 | 28,312 | +38.8% | 4.14 / 5.50 |
| Rate limit theo header, một hot key | 41,012 | 28,545 | +43.7% | 4.27 / 6.01 |

## CPU và bộ nhớ

| Workload | CPU µs/request Prost / nginx | Working set MiB Prost / nginx |
|---|---:|---:|
| Header tĩnh, body 34 byte | 40.69 / 57.94 | 23.34 / 23.70 |
| Header động + request ID + expires | 48.46 / 65.38 | 24.75 / 23.98 |
| ACL + rate limit + header | 39.51 / 57.59 | 24.66 / 24.26 |
| Map exact + condition + expires | 46.76 / 60.28 | 24.97 / 24.58 |
| Map regex + condition + expires | 46.65 / 60.09 | 25.35 / 25.06 |
| Rate limit theo header, một hot key | 42.28 / 57.91 | 25.02 / 24.89 |

Prost dùng working set đỉnh cao hơn nginx ở 5/6 workload. Đây là số đo cgroup (`memory.current - inactive_file`), không phải heap/RSS riêng. Lấy mẫu mỗi 50 ms có thể bỏ lỡ spike. Mỗi proxy giữ cùng process qua nhiều workload trong một vòng, nên cache/buffer còn lại từ ca trước ảnh hưởng RAM.

## Phạm vi policy workload

Các ca trong báo cáo này dùng body 34 byte trên HTTPS/2, 32 connection × 4 stream. Map exact và regex nhận Origin khác nhau để chọn đúng nhánh, rewrite request header, kiểm tra giá trị backend echo, thêm response header có điều kiện và expires động. nginx dùng native map/headers để tạo cùng kết quả.

Ca rate limit theo header dùng một API key không rỗng cho toàn bộ request, rate và burst rất cao để đo chi phí admission khi không bị từ chối. Không đo cardinality lớn hoặc throughput của HTTP 429. Isolation, capacity, idle eviction và quota qua hot update được kiểm tra riêng bằng test chức năng.

Working set khoảng 25 MiB trong bộ tải nhỏ này không thay thế số đo body lớn/slow client. Xem [báo cáo workload nặng](native-benchmark.md) cho các giới hạn bộ nhớ và hiệu năng còn lại. Các báo cáo có source hash và image ID khác nhau; không dùng chênh lệch giữa hai lần chạy làm bằng chứng tối ưu RAM.

## Phạm vi kết luận

- Đây là workload local trên máy dùng chung, không CPU pinning hoặc load generator riêng; không suy rộng thành mọi workload/production capacity.
- gRPC dùng fixture byte-framing và grpc-status, không có chi phí protobuf/business handler. Hai proxy đều xác minh TLS upstream và dùng keep-alive; kiến trúc multiplex/pool vẫn khác nhau.
- Direct-backend control nằm trong raw. Protocol của direct client có thể khác protocol upstream sau proxy, nên không dùng direct RPS làm trần tuyệt đối hoặc trừ để suy ra overhead.
- Các thư mục đo trước `native-verified-final` là exploratory; đã phát hiện và sửa ALPN cùng cấu hình keep-alive gRPC. Không dùng chúng để kết luận so sánh cuối.

Source SHA-256: `edf267720a3221e9f0a182b36bbb5dd247ab4778a25bc1b07adc38a01e553b78`. Image ID, limits, config hash và toàn bộ mẫu đo có trong JSON từng vòng.

Lệnh tái hiện: [benchmarks/README.md](../benchmarks/README.md). Hành vi và giới hạn tính năng: [native-headers.md](native-headers.md).
