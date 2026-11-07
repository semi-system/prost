# So sánh Prost, nginx và Apache2 — 2026-11-07

Đã chạy trực tiếp trong Docker Engine trên WSL2, cùng máy Intel i7-10700,
16 logical CPU. Đây là kết quả của các config được lưu trong `benchmarks/`,
không phải kết luận cho mọi cấu hình production.

## Môi trường chung

- Mỗi proxy: **2 CPU quota, 256 MiB RAM**, không pin core riêng.
- Một backend tĩnh chung, trả JSON **34 byte**, cũng giới hạn 2 CPU/256 MiB.
- **10.000 request/lượt, concurrency 100, 3 lượt/proxy**, đổi thứ tự giữa lượt.
- Mỗi lượt khởi tạo proxy mới, warmup 1.000 request ở concurrency 100.
- HTTP plaintext, keep-alive hai phía; không TLS, cache, WASM hoặc access log.
- Chỉ một proxy được phát tải tại một thời điểm; không build đồng thời.
- nginx **1.28.0**, Apache HTTP Server **2.4.65**, Prost **0.1.0**.
- nginx 2 worker; Apache event MPM 2 process × 64 thread; Prost 2 Tokio worker.
- nginx tắt response/request buffering để gần với cơ chế streaming của Prost.
  Pool, thread model và các header xử lý nội bộ vẫn khác nhau giữa các server.
- Apache được load bộ module tối thiểu phục vụ reverse proxy, không phải cấu
  hình distro đầy đủ module. Prost dùng config benchmark không có plugin native.

Backend này thay cho Python demo để tránh độ trễ và backlog của demo. Vì vậy
không so trực tiếp các con số throughput ở đây với bài đo Python trước đó.

## Kết quả đối chiếu bằng ApacheBench

Lệnh tương đương `ab -k -n 10000 -c 100`. ApacheBench gửi **HTTP/1.0 keep-alive**.
Tất cả 12 lượt có đủ **10.000 request, 0 lỗi, 0 non-2xx** và 10.000 keep-alive
request. Response length đều 34 byte. Tổng cộng 120.000 request qua proxy.

Throughput, p99 và CPU time dưới đây là **trung vị của 3 lượt**; RAM là **đỉnh
lấy mẫu cao nhất của cả 3 lượt**, đo riêng container proxy, không cộng backend.

| Proxy | Req/s | p99 | CPU time / 10.000 request | CPU / request | RAM đỉnh |
|---|---:|---:|---:|---:|---:|
| Prost debug | 5.084 | 47 ms | 3,939 CPU-s | 393,9 µs | 15,50 MiB |
| Prost release | 15.143 | 30 ms | 1,350 CPU-s | 135,0 µs | 16,95 MiB |
| nginx 1.28.0 | 42.054 | 4 ms | 0,468 CPU-s | 46,8 µs | 7,33 MiB |
| Apache2 event | 14.813 | 71 ms | 1,474 CPU-s | 147,4 µs | 20,47 MiB |

CPU-s là thời gian CPU cộng dồn trên các core, không phải thời gian chờ thực tế.
Một request ở đây bao gồm toàn bộ chi phí CPU proxy cho bài tải, không chỉ routing.
AB báo percentile theo mili giây nguyên; p99 không được gộp từ mọi request của ba lượt.

Throughput từng lượt để thấy độ dao động:

| Proxy | Lượt 1 | Lượt 2 | Lượt 3 |
|---|---:|---:|---:|
| Prost debug | 4.919 | 5.084 | 5.458 |
| Prost release | 15.848 | 14.709 | 15.143 |
| nginx | 40.115 | 42.227 | 42.054 |
| Apache2 | 8.949 | 15.530 | 14.813 |

Backend trực tiếp đạt khoảng **112.154–124.813 req/s** trong hai lượt kiểm soát.
Trong phép đo này backend có dư throughput so với cả ba proxy.

Nhận xét theo đúng workload đã đo:

- nginx nhanh hơn Prost release khoảng **2,78×**, dùng khoảng **2,88× ít CPU
  time/request** và **2,31× ít RAM đỉnh lấy mẫu**.
- Prost release có throughput gần Apache event; chênh khoảng 2,2% không đủ để
  khẳng định ưu thế chắc chắn, nhất là Apache có một lượt chậm hơn rõ rệt.
  Prost dùng khoảng 8% ít CPU/request và 17% ít RAM đỉnh hơn trong mẫu này.
- Release tăng throughput khoảng **2,98×** so với debug và giảm CPU/request
  khoảng 66%. Không có thay đổi thuật toán proxy giữa hai bản build.

## Kết quả với bộ phát tải Python HTTP/1.1

Để giữ tương thích với bài đo trước, cũng chạy 3 × 10.000 request/proxy bằng
`scripts/loadtest.py` với HTTP/1.1 và xác minh JSON từng response; tất cả 120.000
request đều thành công.

| Proxy | Req/s trung vị | CPU / request | RAM đỉnh |
|---|---:|---:|---:|
| Prost debug | 5.224 | 383,3 µs | 15,99 MiB |
| Prost release | 16.009 | 88,5 µs | 11,47 MiB |
| nginx | 12.876 | 62,8 µs | 6,33 MiB |
| Apache2 | 11.193 | 149,6 µs | 18,49 MiB |

Gọi trực tiếp backend bằng Python chỉ đạt khoảng **14.940–14.951 req/s**, thấp
hơn rất nhiều so với ApacheBench. Vì vậy **không dùng bảng Python để kết luận
Prost nhanh hơn nginx**. Bộ phát tải, cách lên lịch và giao thức khác nhau làm
kết quả thay đổi; kiểm tra chéo đã phát hiện giới hạn quan trọng của bộ đo ban đầu.

## Giới hạn và cách chạy lại

CPU lấy từ `cpu.stat`; RAM working set là `memory.current - inactive_file`,
lấy mẫu mỗi 50 ms, bao gồm toàn bộ worker/process con trong container. Không
phải heap Rust hoặc tổng RSS. Page dùng chung có thể được tính cho container
nạp trước; đỉnh RAM rất ngắn có thể bị bỏ lỡ. JSON lưu thêm anonymous memory.

Test 10.000 request rất ngắn, chịu ảnh hưởng CFS quota và scheduler; CPU trung
bình có thể hơi vượt 200% ở cửa sổ dưới một giây dù quota là 2 CPU. Máy phát
tải dùng chung host với proxy/backend. Các phép đo chưa bao phủ TLS, payload
lớn, plugin, config update dưới tải, soak hoặc máy phát tải riêng.

Config và hướng dẫn tái lập: [benchmarks/README.md](../benchmarks/README.md).

```sh
sudo python3 scripts/build-release-benchmark.py
sudo python3 scripts/compare-proxies.py --rounds 3
sudo python3 scripts/compare-proxies.py --rounds 3 --driver ab
```

Raw report trên workspace:

- `test-results/comparison/summary.json`: Python HTTP/1.1.
- `test-results/comparison-ab/summary.json`: ApacheBench.
- Mỗi thư mục có JSON từng lượt, samples cgroup, image ID và resource limits;
  bản AB chứa nguyên output ApacheBench, gồm count, lỗi và keep-alive.

Benchmark containers đã dừng sau khi đo; demo thông thường vẫn chạy và không
bị đổi cấu hình.
