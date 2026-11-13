# Cân bằng tải với bốn backend — 2026-11-13

Mỗi proxy hoàn tất **120,000 request đo**, 3 vòng; lỗi Prost 0, nginx 0.

Raw: `test-results/passive-benchmark`. Throughput/p99/CPU dùng trung vị; RAM là đỉnh working set lấy mẫu lớn nhất.

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
| Round-robin, passive policy bật | 28,906 | 23,900 | +20.9% | 5.47 / 7.54 |
| Least-conn, passive policy bật | 29,883 | 25,906 | +15.4% | 5.75 / 5.90 |

Biên throughput giữa các vòng (req/s); các lần đo ngắn trên cùng host còn dao động:

| Workload | Prost min–max | nginx min–max |
|---|---:|---:|
| Round-robin, passive policy bật | 25,272–38,975 | 22,518–26,325 |
| Least-conn, passive policy bật | 28,373–35,740 | 25,477–26,866 |

## CPU và RAM

| Workload | CPU µs/request Prost / nginx | Working set MiB Prost / nginx |
|---|---:|---:|
| Round-robin, passive policy bật | 52.27 / 71.46 | 17.87 / 16.23 |
| Least-conn, passive policy bật | 51.64 / 63.07 | 18.66 / 12.12 |

Working set là memory.current trừ inactive_file, không phải RSS/heap riêng. Mẫu cách 50 ms có thể bỏ lỡ spike. Các case dùng process proxy mới nên không so RAM trực tiếp với các bộ tải khác có vòng đời/buffer khác.

## Phân phối tới backend

| Workload / proxy | node-a | node-b | node-c | node-d |
|---|---:|---:|---:|---:|
| rr_passive / prost | 15,000 | 15,000 | 15,000 | 15,000 |
| rr_passive / nginx | 15,000 | 15,000 | 15,000 | 15,000 |
| least_passive / prost | 15,202 | 14,764 | 14,818 | 15,216 |
| least_passive / nginx | 15,039 | 14,999 | 15,011 | 14,951 |

## Passive policy trên tải healthy

Bốn backend trả 200. Prost bật max_fails=3, fail_timeout_ms=1000, failure_statuses=[502,503,504]. nginx dùng zone, max_fails=3 fail_timeout=1s và proxy_next_upstream error timeout http_502 http_503 http_504 với proxy_next_upstream_tries=1 để không replay request. Tổng counter backend phải đúng số request client.

Đây là đo đường chạy healthy khi policy đã bật, không đo độ trễ ejection/recovery. Hai bên không có health semantics giống hệt: Prost đếm trong cửa sổ trượt và yêu cầu active probe sau cooldown; nginx OSS cho thử traffic lại theo fail_timeout. Các test fault HTTP/TCP và CLI được chạy riêng; không suy ra failover nhanh hơn từ bảng req/s này.

## Đối chứng đường chạy mặc định

So image trước/sau trên cùng config không dùng thuật toán mới, luân phiên thứ tự image/case. Mỗi ca/vòng 100,000 request, 5 vòng; raw: `test-results/passive-header-control`.

| Control | Throughput trước → sau | Chênh | p99 trước → sau ms |
|---|---:|---:|---:|
| native_header_34 | 39,918 → 38,137 | -4.5% | 10.17 → 9.57 |
| native_full_34 | 34,334 → 36,173 | +5.4% | 9.60 → 8.34 |

Gate throughput giảm trên 5% hoặc p99 tăng trên 10%: không có ca vượt ngưỡng trong phép đo này.

CPU affinity của đối chứng: `{'client': '0,2', 'proxy': '4,6', 'backends': ['8,10']}`. Không so p99 tuyệt đối giữa phép đối chứng và các lần đo không ghim CPU.

## Môi trường

CPU: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz; kernel `6.18.54.1-microsoft-standard-WSL2`; `h2load nghttp2/1.68.0`.
Phiên bản: `prost-cli 0.1.0`; `nginx version: nginx/1.28.0`.

Source SHA-256: `4bb8ded517d68d35c77298f9c6f02be34352f3d15ed793caa99b1ea8bef770be`. Các JSON từng vòng ghi image ID, limits, phân phối và mẫu cgroup.

Kết quả chỉ bao phủ fixture nhỏ này; chưa thay thế workload body lớn, soak 24 giờ hoặc profiling production. Hành vi và giới hạn: [load-balancing.md](load-balancing.md). Lệnh tái hiện: [benchmarks/README.md](../benchmarks/README.md).
