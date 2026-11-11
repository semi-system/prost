# Benchmark native header và workload nặng — 2026-11-11

Mỗi proxy hoàn tất **300,000 request đo** trên 5 workload, 3 vòng. Lỗi: Prost 0, nginx 0.

Raw: `test-results/native-rewrite-optimized-final/`. Bảng dùng trung vị các vòng; RAM là đỉnh working set lấy mẫu lớn nhất.

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
| Header tĩnh, body 34 byte | 46,416 | 30,563 | +51.9% | 3.96 / 5.51 |
| Header động + request ID + expires | 33,347 | 27,619 | +20.7% | 4.95 / 5.97 |
| Rewrite exact → /bytes/34 | 39,702 | 31,929 | +24.3% | 4.65 / 4.86 |
| Rewrite prefix → /bytes/34 | 44,376 | 32,694 | +35.7% | 4.11 / 5.07 |
| Rewrite regex + capture → /bytes/34 | 39,966 | 31,536 | +26.7% | 4.19 / 5.41 |

## CPU và bộ nhớ

| Workload | CPU µs/request Prost / nginx | Working set MiB Prost / nginx |
|---|---:|---:|
| Header tĩnh, body 34 byte | 41.79 / 58.68 | 24.61 / 23.16 |
| Header động + request ID + expires | 48.04 / 64.60 | 25.02 / 23.69 |
| Rewrite exact → /bytes/34 | 40.40 / 57.29 | 25.19 / 23.77 |
| Rewrite prefix → /bytes/34 | 41.36 / 58.89 | 25.08 / 24.16 |
| Rewrite regex + capture → /bytes/34 | 41.86 / 58.78 | 24.80 / 24.38 |

Prost dùng working set đỉnh cao hơn nginx ở 5/5 workload. Đây là số đo cgroup (`memory.current - inactive_file`), không phải heap/RSS riêng. Lấy mẫu mỗi 50 ms có thể bỏ lỡ spike. Mỗi proxy giữ cùng process qua nhiều workload trong một vòng, nên cache/buffer còn lại từ ca trước ảnh hưởng RAM.

## Phạm vi rewrite workload

Cả ba ca rewrite dùng HTTPS/2, body 34 byte, 32 connection × 4 stream và forward tới /bytes/34. nginx dùng proxy_pass có URI tĩnh/thay prefix cho hai ca đầu, và native rewrite + capture PCRE cho ca regex. Prost dùng plugin rewrite native với matcher tương ứng. Hai ca header là control cùng lần chạy; không có thay đổi URI trong hai ca đó.

Direct-backend control gọi /bytes/34 trực tiếp. Nếu proxy bỏ qua rewrite, fixture nhận path gốc sẽ trả body kích thước khác và preflight sẽ fail. Không benchmark redirect ở đây vì nginx mặc định tạo body HTML trong khi Prost trả body rỗng; status/Location, query, ACL/rate ordering và streaming được kiểm tra bằng test chức năng.

Số đo này không đại diện body lớn, nhiều client chậm hoặc sức chứa tối đa TLS handshake/SNI. Xem [báo cáo workload nặng](native-benchmark.md) và [hành vi rewrite](rewrite.md).

## Đối chứng đường xử lý không dùng rewrite

Đã kiểm tra image trước thay đổi và image hiện tại trên cùng config, luân phiên thứ tự image và thứ tự workload. Mỗi ca/vòng 100,000 request, 3 vòng, 600,000 request mỗi image, không build hoặc chạy nginx đồng thời. Các plugin rewrite trong fixture được thay bằng header filter rỗng cho cả hai image để bản cũ đọc được, giữ nguyên số route. Đây là phép đo riêng với bảng nginx ở trên.

Bản đầu đã vượt ngưỡng regression, vì vậy đã tách xử lý URI sang future riêng chỉ cấp phát khi dùng, giữ metadata URI mới ngoài context mặc định và dùng Arc cho dữ liệu redirect. Sau thay đổi chạy lại cả test/smoke và phép đối chứng.

| Ca | Chênh throughput bản đầu | Throughput trước → hiện tại | Chênh hiện tại | p99 trước → hiện tại (ms) |
|---|---:|---:|---:|---:|
| Header tĩnh, body 34 byte | -8.5% | 42,411 → 42,720 | +0.7% | 4.19 → 4.19 |
| Header động + request ID + expires | -7.9% | 34,742 → 42,212 | +21.5% | 5.22 → 4.37 |

Image trước: `sha256:183d2756cf6edfeae9182d23f638889b1afde58e7880cd23e06847ad4f37655b`. Image hiện tại: `sha256:97e894304bfd55aa8bd8995c89d421a8a428b346680f2981d8fa5c8c14a9a3d7`.

Raw đối chứng: `test-results/rewrite-interleaved-optimized`. Không có ca vượt ngưỡng throughput giảm 5% hoặc p99 tăng 10% trong phép đo hiện tại. Máy dùng chung vẫn có dao động giữa các vòng; không quy toàn bộ chênh lệch throughput thành lợi ích CPU của một thay đổi cụ thể.

## Phạm vi kết luận

- Đây là workload local trên máy dùng chung, không CPU pinning hoặc load generator riêng; không suy rộng thành mọi workload/production capacity.
- Direct-backend control nằm trong raw. Protocol của direct client có thể khác protocol upstream sau proxy, nên không dùng direct RPS làm trần tuyệt đối hoặc trừ để suy ra overhead.
- Các thư mục đo trước `native-verified-final` là exploratory; đã phát hiện và sửa ALPN cùng cấu hình keep-alive gRPC. Không dùng chúng để kết luận so sánh cuối.

Source SHA-256: `4504da41d0019572aa31f7a97ae9e898807cc97a0ad877af5f8f16764d96a4eb`. Image ID, limits, config hash và toàn bộ mẫu đo có trong JSON từng vòng.

Lệnh tái hiện: [benchmarks/README.md](../benchmarks/README.md). Hành vi và giới hạn tính năng: [native-headers.md](native-headers.md).
