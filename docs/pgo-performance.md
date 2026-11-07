# PGO và benchmark đối chiếu nginx

Đo ngày 2026-11-07 trong Docker trên WSL2, Intel i7-10700. Mỗi proxy và backend
có quota 2 CPU, giới hạn 256 MiB. Driver ApacheBench chạy trên cùng máy, ngoài
quota proxy. Mỗi bản chạy năm vòng, mỗi vòng 1.000 request warmup và 100.000
request đo, concurrency 100; thứ tự proxy luân phiên. Không build/profile đồng
thời với benchmark.

Workload là HTTP plaintext keep-alive, JSON 34 byte, không plugin, không TLS,
không response cache. Backend nginx riêng trả payload cố định. ApacheBench dùng
HTTP/1.0 phía client, HTTP/1.1 phía upstream. Đây là phép đo một workload nhỏ,
không đại diện cho toàn bộ chức năng reverse proxy.

## Kết quả cùng ngân sách pool

Raw: `test-results/pgo-pool-matched-100k/summary.json` và các file từng vòng.

| Chỉ số | Prost PGO | nginx pool 128 | nginx pool 32 |
|---|---:|---:|---:|
| Throughput trung vị, req/s | 51.031 | 45.233 | 42.500 |
| CPU/request, µs | 38,84 | 44,02 | 46,75 |
| p99 trung vị, ms | 4 | 4 | 4 |
| Working set đỉnh lấy mẫu, MiB | 16,95 | 7,10 | 8,05 |
| Lỗi/request | 0/500.000 | 0/500.000 | 0/500.000 |

Prost cao hơn nginx pool 128 khoảng **12,8% throughput**, giảm khoảng **11,8%
CPU/request** trong phép đo này. Bộ nhớ vẫn cao hơn. Working set là
`memory.current - inactive_file`, lấy mẫu mỗi 50 ms, gồm cả container; đây không
phải heap riêng của chương trình và có thể bị ảnh hưởng bởi cách cgroup tính
trang file dùng chung. p99 của ApacheBench chỉ có độ phân giải millisecond.

nginx 1.28.0 là binary chính thức của image `nginx:stable-bookworm`, hai worker.
Config pool 128 có 64 idle connection/worker, tương ứng tổng budget của hai
worker Prost. Config gốc có 16/worker. Cả hai tắt buffering và retry. Việc tăng
pool cũng giúp nginx nhanh hơn; dùng config gốc để kết luận duy nhất sẽ thiếu
công bằng. Image ID và giới hạn tài nguyên nằm trong báo cáo từng vòng.

## Đối chiếu bổ sung với nginx bật reuseport

Đã đo riêng năm vòng nữa với nginx pool 128 và `listen ... reuseport`, cùng
cơ chế phân phối accept theo worker của Prost. Raw:
`test-results/pgo-reuseport-100k/summary.json`.

| Chỉ số | Prost PGO | nginx pool 128 + reuseport |
|---|---:|---:|
| Throughput trung vị, req/s | 51.121 | 44.527 |
| CPU/request, µs | 38,93 | 44,73 |
| p99 trung vị, ms | 4 | 4 |
| Working set đỉnh lấy mẫu, MiB | 14,64 | 7,27 |
| Lỗi/request | 0/500.000 | 0/500.000 |

Chênh lệch throughput khoảng **14,8%**, CPU/request giảm khoảng **13,0%**.
Mọi vòng Prost đạt 50.047–51.583 req/s, nginx đạt 42.050–46.299 req/s.
Không kết luận reuseport làm nginx chậm hơn: hai tập đo khác thời điểm, không
có CPU pinning và vẫn có nhiễu từ máy dùng chung. Cả hai cấu hình nginx đã thử
đều thấp hơn Prost PGO trong workload này.

## Tác dụng riêng của PGO

Trong lượt đối chiếu trước (`test-results/pgo-final-100k/`, cũng năm vòng ×
100.000 request), release thường đạt 40.859 req/s, PGO 50.744 req/s và nginx
pool 32 đạt 42.270 req/s. PGO cải thiện khoảng 24,2% so với release thường.
Các thay đổi runtime, pool, body và metrics được mô tả ở
[performance-optimization.md](performance-optimization.md).

[Profile-guided optimization của rustc](https://doc.rust-lang.org/rustc/profile-guided-optimization.html)
dùng dữ liệu chạy instrumented để hướng dẫn compiler tối ưu bản release cuối.
Không phải cache response hoặc bỏ qua upstream. Bản cuối không chứa các section
instrumentation `__llvm_prf`. Không dùng `target-cpu=native`.

Corpus hiện tại: 50.000 request HTTP/1.0 và 50.000 HTTP/1.1, payload 34 byte.
Source hash `40222cfb846d91b9`, Rust 1.93.1, target
`x86_64-unknown-linux-gnu`. Manifest nằm ở
`test-results/pgo/40222cfb846d91b9/manifest.json`.

Đây là lợi thế của binary được train cho workload này so với binary nginx phân
phối sẵn. Chưa so với nginx tự build PGO. Chưa xác nhận lợi ích cho HTTPS, HTTP/2,
gRPC, body lớn hoặc WASM. Image PGO là lựa chọn riêng, không thay mặc định image
release portable.

## Tái hiện trong WSL

Từ thư mục gốc, với Docker daemon đang chạy và `apache2-utils` đã cài:

```sh
docker build -f Dockerfile.dev -t prost-dev .
docker build -t prost:mvp .
python3 scripts/build-pgo.py
python3 scripts/smoke-image.py --image prost:bench-pgo
python3 scripts/compare-proxies.py --rounds 5 --requests 100000 --driver ab \
  --targets prost-pgo nginx-tuned nginx \
  --output-dir test-results/my-pgo-comparison
```

Build PGO dùng volume Linux `prost-pgo-target` và `prost-pgo-data`, tách khỏi
build bình thường. Script tự tạo image SDK có LLVM tools, train, merge profile
và build binary không instrumented. Thay Rust/Cargo cần train lại; profile được
phân thư mục theo hash source. Không chạy nhiều bản script đồng thời vì tên
container compile và project benchmark cố định.

22 test đã pass cả cấu hình thường và cấu hình release dùng profile này; smoke
image PGO cũng pass, gồm cập nhật config qua Runtime API. Các kiểm tra này xác
nhận chức năng, chưa thay thế soak 24 giờ hay fault testing production.

## Hướng tiếp theo

- Đo HTTPS, HTTP/2, gRPC, payload 1 KiB–1 MiB và streaming trước khi mở rộng corpus
  PGO; giữ một tập workload độc lập để tránh tối ưu quá sát dữ liệu train.
- Profile allocation và retention để giảm RAM còn cao hơn nginx. Chỉ đổi allocator
  sau khi có số đo heap, CPU và fragment dưới tải dài.
- Đo worker scaling 1/2/4/8 trên CPU cố định và load generator riêng; hiện quota
  CPU không phải CPU pinning, WSL và máy dùng chung vẫn tạo nhiễu.
- Giữ Runtime API, backpressure, timeout và generation semantics làm điều kiện
  bắt buộc; kiểm tra regression theo từng giao thức.
