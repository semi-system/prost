# Cân bằng tải với bốn backend — 2026-11-13

Mỗi proxy hoàn tất **120,000 request đo**, 3 vòng; lỗi Prost 0, nginx 0.

Raw: `test-results/capacity-benchmark-final`. Throughput/p99/CPU dùng trung vị; RAM là đỉnh working set lấy mẫu lớn nhất.

## Điều kiện

- Docker trong WSL2, cùng fixture Go cho bốn backend riêng biệt. Mỗi proxy 2 CPU/256 MiB, mỗi backend 1 CPU/128 MiB, GOMAXPROCS=1.
- HTTPS/2 downstream, HTTP/1.1 upstream, body cố định 34 byte; 32 connection × 4 stream, 2 load threads; warmup 256 request.
- Proxy và case chạy luân phiên; không build hay chạy proxy đối thủ đồng thời. Client/server vẫn cùng host, không có load generator riêng.
- Không cache, access log, response/request buffering hoặc retry. nginx dùng zone để chia sẻ trạng thái giữa worker; Prost có active /ready checks, không tính vào counter traffic của fixture.
- Idle budget 512 qua bốn địa chỉ: Prost tối đa 128/host, nginx 256/worker cho cả group. Phân bổ per-host và shared-group khác nhau, là một phần của workload.
- Trước tải kiểm tra byte body, protocol upstream và affinity mẫu; từng request đo phải có status 200, đủ mẫu latency và đủ byte. Counter bốn backend phải cộng đúng số request đo.
- Counter /stats không tính health/readiness và chính nó. Metadata cgroup/image ID/config đã lưu để kiểm tra budget và backend thực sự nhận traffic.

CPU affinity trong guest: client `0,2`, proxy `4,6`, bốn backend `['8', '10', '12', '14']`. Chọn logical CPU đầu tiên của các core khác nhau theo lscpu; không bảo đảm cô lập vật lý khỏi Windows/hypervisor.

## Throughput và latency

| Workload | Prost req/s | nginx req/s | Chênh Prost | p99 Prost / nginx ms |
|---|---:|---:|---:|---:|
| Round-robin, primary cap 16 + ba backup | 32,387 | 26,198 | +23.6% | 9.43 / 10.41 |
| Least-conn, primary cap 16 + ba backup | 28,748 | 26,154 | +9.9% | 9.79 / 10.44 |

Biên throughput giữa các vòng (req/s); các lần đo ngắn trên cùng host còn dao động:

| Workload | Prost min–max | nginx min–max |
|---|---:|---:|
| Round-robin, primary cap 16 + ba backup | 29,317–34,735 | 23,476–26,362 |
| Least-conn, primary cap 16 + ba backup | 26,312–30,057 | 23,524–26,843 |

## CPU và RAM

| Workload | CPU µs/request Prost / nginx | Working set MiB Prost / nginx |
|---|---:|---:|
| Round-robin, primary cap 16 + ba backup | 52.03 / 71.04 | 17.50 / 16.45 |
| Least-conn, primary cap 16 + ba backup | 51.94 / 65.35 | 17.68 / 12.74 |

Working set là memory.current trừ inactive_file, không phải RSS/heap riêng. Mẫu cách 50 ms có thể bỏ lỡ spike. Các case dùng process proxy mới nên không so RAM trực tiếp với các bộ tải khác có vòng đời/buffer khác.

## Phân phối tới backend

| Workload / proxy | node-a | node-b | node-c | node-d |
|---|---:|---:|---:|---:|
| rr_backup / prost | 4,051 | 18,654 | 18,627 | 18,668 |
| rr_backup / nginx | 4,855 | 18,382 | 18,382 | 18,381 |
| least_backup / prost | 4,522 | 18,508 | 18,466 | 18,504 |
| least_backup / nginx | 4,889 | 18,391 | 18,364 | 18,356 |

## Primary giới hạn và backend dự phòng

node-a là primary duy nhất, delay trước response header 5 ms, cap 16. node-b/c/d là backup không đặt cap riêng. Prost dùng max_active=16, nginx max_conns=16 + zone. Upstream HTTP/1.1 không multiplex; đây vẫn là hai đơn vị cap khác nhau, không suy rộng sang HTTP/2 upstream.

Preflight tuần tự phải chỉ vào primary. Tải 128 stream phải có request vào cả primary và backup; counters backend phải cộng đủ số request và mọi status/body phải đúng. Đây là tải spillover khi primary đầy, không phải lỗi backend/crash hay benchmark thời gian phát hiện health failure.

## Đối chứng đường chạy mặc định

So image trước/sau trên cùng config không dùng thuật toán mới, luân phiên thứ tự image/case. Mỗi ca/vòng 100,000 request, 5 vòng; raw: `test-results/capacity-header-final`.

| Control | Throughput trước → sau | Chênh | p99 trước → sau ms |
|---|---:|---:|---:|
| native_header_34 | 40,904 → 39,776 | -2.8% | 9.26 → 8.79 |
| native_full_34 | 33,400 → 36,579 | +9.5% | 8.03 → 8.20 |

Gate throughput giảm trên 5% hoặc p99 tăng trên 10%: không có ca vượt ngưỡng trong phép đo này.

CPU affinity của đối chứng: `{'client': '0,2', 'proxy': '4,6', 'backends': ['8,10']}`. Không so p99 tuyệt đối giữa phép đối chứng và các lần đo không ghim CPU.

Đối chứng trước có ghim CPU (`test-results/capacity-header-control`) ghi nhận native_header_34: throughput -2.4%, p99 +13.7%. Đã điều tra nhánh round-robin và điều chỉnh fast path trước lần đo hiện tại. Giữ raw của lần vượt ngưỡng để kiểm tra; không quy mọi chênh lệch giữa các lần đo thành hiệu quả của code, vì tải cùng host vẫn dao động.

## Môi trường

CPU: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz; kernel `6.18.54.1-microsoft-standard-WSL2`; `h2load nghttp2/1.68.0`.
Phiên bản: `prost-cli 0.1.0`; `nginx version: nginx/1.28.0`.

Source SHA-256: `e53dcf01a13ac897112ad04ccf504d6eea35892d98ca8991ca7b3078373c63ed`. Các JSON từng vòng ghi image ID, limits, phân phối và mẫu cgroup.

Kết quả chỉ bao phủ fixture nhỏ này; chưa thay thế workload body lớn, soak 24 giờ hoặc profiling production. Hành vi và giới hạn: [load-balancing.md](load-balancing.md). Lệnh tái hiện: [benchmarks/README.md](../benchmarks/README.md).
