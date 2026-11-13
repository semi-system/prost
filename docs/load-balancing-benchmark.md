# Cân bằng tải với bốn backend — 2026-11-12

Mỗi proxy hoàn tất **300,000 request đo**, 3 vòng; lỗi Prost 0, nginx 0.

Raw: `test-results/balance-pinned-final`. Throughput/p99/CPU dùng trung vị; RAM là đỉnh working set lấy mẫu lớn nhất.

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
| Round-robin, backend đồng đều | 35,238 | 25,995 | +35.6% | 4.28 / 5.58 |
| Least-conn, backend đồng đều | 33,786 | 27,085 | +24.7% | 4.78 / 6.17 |
| Round-robin, một backend chậm 5 ms | 30,606 | 24,058 | +27.2% | 9.08 / 9.65 |
| Least-conn, một backend chậm 5 ms | 32,081 | 25,110 | +27.8% | 9.26 / 10.44 |
| Consistent hash, backend đồng đều | 32,160 | 27,704 | +16.1% | 6.04 / 6.49 |

## CPU và RAM

| Workload | CPU µs/request Prost / nginx | Working set MiB Prost / nginx |
|---|---:|---:|
| Round-robin, backend đồng đều | 49.22 / 68.80 | 17.11 / 16.14 |
| Least-conn, backend đồng đều | 49.19 / 66.29 | 16.42 / 11.72 |
| Round-robin, một backend chậm 5 ms | 58.84 / 73.38 | 18.11 / 12.68 |
| Least-conn, một backend chậm 5 ms | 52.91 / 65.87 | 16.54 / 12.16 |
| Consistent hash, backend đồng đều | 51.32 / 67.69 | 21.68 / 12.54 |

Working set là memory.current trừ inactive_file, không phải RSS/heap riêng. Mẫu cách 50 ms có thể bỏ lỡ spike. Các case dùng process proxy mới nên không so RAM trực tiếp với các bộ tải khác có vòng đời/buffer khác.

## Phân phối tới backend

| Workload / proxy | node-a | node-b | node-c | node-d |
|---|---:|---:|---:|---:|
| rr_uniform / prost | 15,000 | 15,000 | 15,000 | 15,000 |
| rr_uniform / nginx | 15,000 | 15,000 | 15,000 | 15,000 |
| least_uniform / prost | 14,878 | 15,053 | 15,034 | 15,035 |
| least_uniform / nginx | 14,981 | 14,987 | 15,000 | 15,032 |
| rr_slow / prost | 15,000 | 15,000 | 15,000 | 15,000 |
| rr_slow / nginx | 15,000 | 15,000 | 15,000 | 15,000 |
| least_slow / prost | 4,374 | 18,551 | 18,478 | 18,597 |
| least_slow / nginx | 5,347 | 18,228 | 18,211 | 18,214 |
| hash_uniform / prost | 13,056 | 16,992 | 16,416 | 13,536 |
| hash_uniform / nginx | 17,760 | 15,264 | 11,712 | 15,264 |

Prost least-conn giảm phần request vào node-a chậm từ 25.0% xuống 7.3%. Throughput thay đổi +4.8% so với round-robin, nên không suy ra rằng chuyển tải khỏi node chậm luôn tăng throughput/p99. Tải này còn gồm TLS, scheduling, pool và chi phí giữ load lease.

Hash dùng corpus 256 request-URI; driver tuần hoàn URI trên từng client. Preflight kiểm tra lặp lại 16 key. SHA-256/ring của Prost và ketama của nginx cho mapping khác nhau, như bảng phân phối cho thấy. Các backend hash đều không có delay nhân tạo, nhưng đây vẫn là phép đo toàn bộ routing/pool, không cô lập chi phí hàm hash.

## Đối chứng đường chạy mặc định

So image trước/sau trên cùng config không dùng thuật toán mới, luân phiên thứ tự image/case. Mỗi ca/vòng 100,000 request, 5 vòng; raw: `test-results/balance-header-pinned`.

| Control | Throughput trước → sau | Chênh | p99 trước → sau ms |
|---|---:|---:|---:|
| native_header_34 | 40,797 → 39,051 | -4.3% | 8.38 → 8.33 |
| native_full_34 | 33,498 → 34,096 | +1.8% | 8.08 → 7.47 |

Gate throughput giảm trên 5% hoặc p99 tăng trên 10%: không có ca vượt ngưỡng trong phép đo này.

CPU affinity của đối chứng: `{'client': '0,2', 'proxy': '4,6', 'backends': ['8,10']}`. Không so p99 tuyệt đối giữa phép đối chứng và các lần đo không ghim CPU.

Lần đo trước không ghim CPU (`test-results/balance-header-control`) ghi nhận native_full_34: throughput -9.8%, p99 +8.9%. Vì vượt ngưỡng đã tách việc dựng key/hash/least-load khỏi fast path round-robin, chạy lại test/smoke và đối chứng 5 vòng có ghim CPU. Các raw cũ được giữ; không quy toàn bộ khác biệt giữa hai phương pháp đo thành hiệu quả của một thay đổi code.

## Môi trường

CPU: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz; kernel `6.18.54.1-microsoft-standard-WSL2`; `h2load nghttp2/1.68.0`.
Phiên bản: `prost-cli 0.1.0`; `nginx version: nginx/1.28.0`.

Source SHA-256: `12286e9f0ecb1729dea29822fa65e381de2204dc79bb3b7e7cba3c5de39d5d78`. Các JSON từng vòng ghi image ID, limits, phân phối và mẫu cgroup.

Kết quả chỉ bao phủ fixture nhỏ này; chưa thay thế workload body lớn, soak 24 giờ hoặc profiling production. Hành vi và giới hạn: [load-balancing.md](load-balancing.md). Lệnh tái hiện: [benchmarks/README.md](../benchmarks/README.md).
