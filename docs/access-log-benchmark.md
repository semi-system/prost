# Access log: correctness, tài nguyên và chi phí

Đo local Docker Engine trong WSL2, 2026-11-14. Native release Rust 1.93, locked,
WASM tắt, portable thin LTO; không chạy build hoặc nginx đồng thời. Các comparison
ghi nhận không có container khác đang chạy ở đầu bài; host Windows vẫn có thể
gây nhiễu. Mỗi proxy 2 CPU, 256 MiB, 2 I/O workers; backend Go riêng cùng budget.
Không pin CPU; không phải số đo phần cứng production hoặc so sánh trực tiếp nginx.

## Kiểm thử chức năng

116 test qua ở cả default features và all features; một test ignored là fixture
crash được test cha gọi bằng subprocess. Fmt/clippy qua, 4 Python parser test qua.
Docker demo đầy đủ qua: native filters, balancing, backup/capacity/passive health,
HTTP/TCP, transaction, restart và phiên giữ qua apply.

Test mới bao gồm HTTP/1.1–HTTP/2, gRPC trailers, HEAD/204/304, local rejection,
body timeout sau headers, cancel stream, TCP half-close, WebSocket, log rotation,
generation conflict, hot flags, queue đầy và lỗi ghi. Metrics test xác nhận H1
reuse, H2 multiplexing, TLS/ALPN, refused connect, TLS timeout, attempt bị hủy,
pool cũ giữ transport và thu hồi sau stream/idle timeout.

## 10000 request và đầy đĩa thật

Script `scripts/access-log-smoke.py`: 8 H2 connections × 8 streams, H1 upstream,
body 34 byte, log mọi request, reuse tracking bật, queue 32. Log nằm trên tmpfs
riêng 64 KiB để ép ENOSPC; không làm đầy đĩa host. Đây là bài correctness/resource,
writer ngừng ghi sau lỗi nên **không dùng RPS bài này làm benchmark logging**.

- **10000/10000 request thành công**, 0 failed/error/timeout; nhận đúng 340000 byte DATA.
- Cgroup working set đỉnh **9.64 MiB**, CPU proxy **46.83 µs/request** trong bài này.
- RSS process sau tải **14.09 MiB**; RSS và cgroup working set là hai phép đo khác nhau.
- FD chính đỉnh 103, sau idle 25; còn 0 downstream connection, 1 live generation.
- Writer sống, 1 lỗi I/O, 9833 record thuộc nhóm I/O không chắc chắn, 0 record bỏ do queue đầy.
- Flush báo lỗi khi sink ngừng; truncate/reopen phục hồi, 8 JSON record hợp lệ,
  query không lọt vào log. Rotate/reopen lần hai, generation conflict, hot-disable
  và restart giữ đúng flag đều qua.

## Đường mặc định: log/reuse tracking tắt

Ba round xen kẽ thứ tự image và case, 8 giây/case/round, 32 H2 connections × 4
streams; median RPS và p99. Cùng cấu hình có hash
`2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0`. Connector counters vẫn bật ở bản mới.

| Workload | RPS cũ → mới | Δ RPS | p99 cũ → mới (ms) | Δ p99 |
|---|---:|---:|---:|---:|
| `native_header_34` | 46,449 → 46,828 | +0.82% | 5.254 → 4.418 | -15.91% |
| `h2_64k` | 13,023 → 12,446 | -4.43% | 13.805 → 16.444 | +19.12% |
| `double_tls_h2_64k` | 10,996 → 10,684 | -2.84% | 17.284 → 18.952 | +9.65% |

Gate là throughput −5% / p99 +10%; các lượt vượt ngưỡng được ghi riêng bên dưới,
không suy ra zero overhead từ lượt nằm trong gate. Peak memory giữa case có ảnh hưởng thứ tự chạy và allocator giữ buffer;
xem anon/cgroup samples thay vì suy ra heap leak từ một peak working set.

## Chi phí bật log và reuse tracking

Cùng binary mới, off/on xen kẽ ba round, mỗi case 8 giây, cùng giới hạn 2 CPU.
On dùng queue 2048, flush 1000 ms, file append trong volume ext4 Linux. Writer
chia CPU quota với hai traffic workers. Các số dưới đây gồm cả access log và
`observe_connection_reuse=true`, không tách riêng chi phí từng tính năng.

| Policy | Workload | RPS off → on | Δ RPS | p99 off → on (ms) | CPU off → on (µs/request) |
|---|---|---:|---:|---:|---:|
| mọi request | `native_header_34` | 42,642 → 30,047 | -29.54% | 5.102 → 11.158 | 46.87 → 66.64 |
| mọi request | `h2_64k` | 12,116 → 10,767 | -11.14% | 16.558 → 20.880 | 164.64 → 185.97 |
| sample 1/16 | `native_header_34` | 37,976 → 36,851 | -2.96% | 6.519 → 7.546 | 52.35 → 54.31 |
| sample 1/16 | `h2_64k` | 10,977 → 11,186 | +1.91% | 19.034 → 17.364 | 181.62 → 178.85 |

Không có record bị bỏ, I/O error, pending hoặc active ticket tại các lần kiểm tra
sau flush trong hai comparison bật log. Counters lưu trong raw JSON là **cộng
dồn từ startup của round**, gồm readiness/warmup/case trước; không cộng các dòng
đó với nhau. Flush xác nhận buffer đã giao cho filesystem, không đảm bảo fsync.

Full JSON logging vượt gate hiệu năng trên hai case. CPU/request tăng và writer
tham gia cùng quota là chi phí cần tính khi triển khai; gate này đã được điều tra,
không đổi tên thành “đã tối ưu ngang nginx”. Bài này chưa profile tách từng phần
serde/channel/observer/kernel write. Giữ mặc định log tắt; bật logging/sampling
theo nhu cầu vận hành và đo lại trên workload thực tế. Chưa có log format tùy ý,
log có điều kiện theo status, syslog hoặc pool idle/busy/hit-miss chính xác.

Timed load có 128 stream đang bay ở deadline mỗi round/case: đã lưu riêng trong
`unfinished_at_deadline`, không gộp vào completed RPS hoặc gọi là lỗi transport.
Các request hoàn tất đều có byte count đúng và không có lỗi transport được ghi nhận.

## Tái hiện và dữ liệu gốc

```sh
sudo python3 scripts/access-log-smoke.py --image prost:native --output-dir test-results/access-log-final-smoke
sudo python3 scripts/compare-header-images.py \
  --before prost:access-control --after prost:native \
  --rounds 3 --duration 8 \
  --cases native_header_34 h2_64k double_tls_h2_64k \
  --output-dir test-results/access-default-final
```

Với comparison policy, tạo off/on TOML từ fixture native sau
`scripts/prepare-advanced-bench.py`; on thêm `[runtime.access_log]` với path
`/var/lib/prost/access.jsonl`, queue 2048, flush 1000 và sample_every 1 hoặc 16,
đồng thời bật `observe_connection_reuse=true` trong từng upstream. Dùng:

```sh
sudo python3 scripts/compare-header-images.py \
  --before prost:native --after prost:native \
  --before-config off.toml --after-config on.toml --capture-access-log \
  --rounds 3 --duration 8 --cases native_header_34 h2_64k \
  --output-dir test-results/access-enabled-final
```

Raw JSON, stdout h2load, latency TSV, cgroup samples, config và image IDs:

- `test-results/access-log-final-smoke/summary.json`, `metrics.prom`, `traffic.txt`.
- `test-results/access-default-final/summary.json`.
- `test-results/access-enabled-final/summary.json`.
- `test-results/access-sampled-final/summary.json`.

`test-results` không đưa vào Git; report này lưu kết luận và thông số đo.

Image cũ: `sha256:b39b808f5376130bed52ad5ee1b525fe3ea76cd29bab7358976cd5856bf09316`.

Image mới: `sha256:41e2eb6cd03f6b1c0f9b75f99bef59e73a3f505bf9725a7f10d4f92a1bf3f0a7`.

## Điều tra gate và tối ưu context

Bản thử ban đầu lưu policy/ticket trong request extensions; đường này cấp phát map/context ngay cả request bị sampling bỏ qua. Bản cuối truyền snapshot/route/ticket trực tiếp, bỏ các box đó và một Arc clone. Không đổi log schema hoặc snapshot semantics. Raw trước sửa giữ ở `access-sampled-tail-check` (5 round × 12 giây, image `prost:access-initial`): throughput −9.96%, p99 +37.28%; các số đó không phải kết quả binary cuối.

`h2_64k` ở bản cuối vẫn vượt gate: throughput -4.43%, p99 +19.12%. p99 before theo round: 13.498, 15.296, 13.805 ms; p99 after theo round: 16.444, 14.89, 16.641 ms; cần giữ cảnh báo này khi đánh giá triển khai.

- log toàn bộ + reuse, `native_header_34`: throughput -29.54%, p99 +118.70% (vượt gate).
- log toàn bộ + reuse, `h2_64k`: throughput -11.14%, p99 +26.10% (vượt gate).
- sample 1/16 + reuse, `native_header_34`: throughput -2.96%, p99 +15.75% (vượt gate).

### Tách policy ở body 34 byte

Ba round × 8 giây, cùng binary cuối, so với cả hai tính năng tắt. Basic transport counters luôn chạy.

| Chỉ bật | RPS off → on | Δ RPS | p99 off → on (ms) | CPU off → on (µs/request) |
|---|---:|---:|---:|---:|
| reuse tracking | 42,679 → 43,629 | +2.23% | 5.508 → 4.986 | 46.67 → 45.67 |
| log sample 1/16, reuse tắt | 41,124 → 36,834 | -10.43% | 5.016 → 6.855 | 48.47 → 54.30 |

Sampling giảm số record nhưng không làm chi phí logging bằng 0. So sánh riêng vẫn cho thấy overhead và tail latency ở body nhỏ; không quy toàn bộ chênh lệch của comparison kết hợp cho reuse tracking. Các lượt không pin dao động giữa session; raw file giữ từng round để tránh gộp thành một cam kết RPS.

### Đối chứng H2 với CPU pinning

Năm round × 12 giây, client/proxy/backend dùng các physical core riêng: `{'client': '0,2', 'proxy': '4,6', 'backends': ['8,10']}`. Cùng cấu hình, log/reuse tắt. Median RPS 11,942 → 12,374 (+3.61%), p99 16.469 → 18.557 ms (+12.68%).

Raw ở `test-results/access-default-pinned`; không xóa kết quả H2 không pin vượt gate ở trên. CPU pinning thay đổi điều kiện đo nên hai bài được báo riêng.

P99 của đối chứng pin vẫn vượt gate +10%. Throughput/CPU tốt hơn không bù được kết quả tail latency này; **regression p99 chưa được giải quyết**. Cần điều tra lịch chạy/queue/flow control với latency trace trước khi kết luận đạt toàn bộ gate. Không tiếp tục đổi cấu hình đo chỉ để lấy một lượt qua ngưỡng.


### CPU profile của đường mặc định

Thu riêng với `perf cpu-clock:u`, 199 Hz, 8 giây/image và cùng CPU pinning; không
dùng throughput dưới perf làm benchmark. Recovered leaf symbols từ đúng ELF/image
ở `test-results/access-final-cpu`; đây không phải full DWARF/inline call stack.

- Cũ: AES-GCM leaf khoảng 15.70%, `handle_http` closure 5.07%, H2 connection poll 2.36%.
- Mới: AES-GCM leaf khoảng 16.38%, `select_route_index` 5.42%, `handle_http` closure 1.49%.

Route selection được tách ra thành hàm nên phạm vi attribution thay đổi; không
cộng/trừ phần trăm leaf để kết luận regression do một hàm cụ thể. Profile xác
định TLS/routing/Hyper là các điểm nóng, chưa xác định nguyên nhân p99 tăng.
Đây là giới hạn còn lại, không phải xác nhận đạt ngang/vượt nginx.

Các comparison policy riêng nằm ở `test-results/access-component-reuse` và
`test-results/access-component-sample`. Binary/config/image IDs và raw latency
samples đều được lưu. Muốn tái hiện đối chứng H2, thêm `--pin-cpus --rounds 5
--duration 12 --cases h2_64k` vào lệnh comparison mặc định ở trên.
