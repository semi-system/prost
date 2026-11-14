# Metrics vận hành

Idle/busy transport, cache/retry và metrics plugin opt-in:
[mvp-bundle.md](mvp-bundle.md).

Metrics transport HTTP thực tế (open/close/connect/TLS/reuse) và access log:
[access-log.md](access-log.md). Gauge pool bên dưới vẫn chỉ đếm nhóm client.

GC artifact có counters số lượt/kết quả, file/bytes đã unlink và histogram thời gian;
scrape không đọc kho artifact. Chi tiết: [artifact-gc.md](artifact-gc.md).

Bật endpoint loopback tại startup:

```toml
[runtime]
metrics_address = "127.0.0.1:9090"
```

`GET /metrics` trả Prometheus text với Content-Type tương ứng. Với Docker, địa chỉ
này nằm trong network namespace của container, không phải loopback của host.
Agent giám sát cần cùng namespace hoặc cách truy cập nội bộ phù hợp. Runtime
settings vẫn cần restart. Library có thể dùng `proxy.metrics.registry.gather()`;
counters được ghi cả khi không bật HTTP metrics endpoint.

## Nhãn và vòng đời

Chỉ dùng tên upstream/endpoint trong config và tập nhãn cố định `class`, `result`,
`operation`, `reason`. Không gắn URL, IP, request ID, SNI hoặc generation vào nhãn.
Exporter đọc snapshot hiện tại lúc scrape; endpoint bị xóa không còn series trong
response, không giữ registry con theo lịch sử apply. Prometheus tự quản lý retention
của những series nó đã thu thập trước đó.

Endpoint giữ cùng id/address tái sử dụng counter qua thay pool, TLS, protocol,
weight, policy và enabled. Request/probe còn chạy ở generation cũ vẫn cập nhật
counter dùng chung của identity đó, dù health eligibility của generation được
quan sát độc lập. Đổi id/address, xóa rồi thêm lại hoặc restart có thể reset
counter; dùng `rate`/`increase` có xử lý counter reset. Rejection counter giữ theo
tên upstream khi upstream đó còn trong config.

Đường traffic không tạo label, lookup map, lấy mutex metrics hay cấp phát cho
counter. Mỗi endpoint có slot 64 byte/worker. Worker đơn luồng là writer duy nhất
của slot và dùng atomic load/store để scrape vẫn đọc an toàn; runtime chia sẻ dùng
atomic fetch-add. Bộ nhớ counter tỷ lệ endpoint × worker, không tỷ lệ request/client.
Histogram latency HTTP hiện có vẫn batch theo worker. Các family động chỉ được
dựng khi gather; exporter giữ Weak để không giữ daemon hoặc snapshot sống mãi.

## Upstream và health

Các metric sau có nhãn `upstream`, `endpoint`, trừ khi ghi khác:

| Metric | Ý nghĩa |
|---|---|
| `prost_upstream_selections_total` | Số lượt chọn thành công, kể cả sau đó connect lỗi |
| `prost_upstream_responses_total{class}` | Headers HTTP gốc từ backend, class 1xx–5xx, trước response filters |
| `prost_upstream_forward_errors_total` | TCP connect/TLS hoặc HTTP forward lỗi trước Outcome; không đếm thêm lỗi body/relay sau đó |
| `prost_upstream_enabled`, `prost_upstream_backup`, `prost_upstream_healthy` | Gauge 0/1 từ config và health |
| `prost_upstream_max_active` | Cap đã cấu hình; 0 là unlimited |
| `prost_upstream_active` | Request/stream/session còn lease; chỉ xuất khi tracking đã bật |
| `prost_upstream_active_tracking{upstream}` | 0/1, giúp phân biệt không theo dõi với active bằng 0 |
| `prost_upstream_rejections_total{upstream,reason}` | `unavailable` hoặc `capacity`; không tính ACL/rate/global request admission hoặc key không hợp lệ |
| `prost_upstream_health_probes_total{result}` | Probe hoàn tất: success/error; timeout là error, probe bị hủy trước observe không tính |
| `prost_upstream_passive_failures_total` | Passive failure đủ điều kiện; disabled policy không ghi, lỗi in-flight trong isolation vẫn là failure event |
| `prost_upstream_passive_ejections_total` | Ejection do passive policy; không tăng lại cho mỗi lỗi trong cùng cooldown |
| `prost_upstream_passive_ejected` | Gauge passive isolation, chỉ xuất khi có explicit passive policy |
| `prost_upstream_passive_recent_failures` | Timestamp còn trong window, xóa khi chạm ngưỡng |
| `prost_upstream_passive_cooldown_seconds` | Thời gian tối thiểu còn lại; zero vẫn có thể đang chờ active probe |

Selections là tổng cả HTTP và TCP nếu cùng upstream dùng cho nhiều listener.
Responses tính khi nhận headers, không chứng minh body đã tới client. Health
counters bao gồm quan sát của các generation còn sống có cùng identity; gauge
health và cap lấy từ generation hiện hành. Xem [passive-health.md](passive-health.md)
và [upstream-capacity.md](upstream-capacity.md).

## TLS và Runtime API

| Metric | Ý nghĩa |
|---|---|
| `prost_tls_handshakes_total{result}` | Downstream TLS: success/error/timeout/rejected/cancelled |
| `prost_tls_handshake_seconds` | Histogram những handshake đã bắt đầu, gồm lỗi/cancel; không gồm admission bị từ chối |
| `prost_tls_handshakes_active` | Handshake permits đang dùng, tối đa 256 |
| `prost_config_transactions_total{operation,result}` | apply/endpoint_patch/validate; success/not_durable/conflict/error/cancelled |
| `prost_config_transaction_seconds{operation}` | Histogram toàn thao tác, gồm thời gian chờ coordinator |

Apply/patch được ghi trong task coordinator đã detach, nên client ngắt kết nối
không làm mất kết quả metric. `not_durable` là commit đã publish nhưng directory
fsync sau rename không thành công; xem semantics persistence trong architecture.
`cancelled` cũng bao gồm future bị drop/unwind trước khi có kết quả cuối. Validate
không publish nhưng vẫn có metric riêng. TLS metrics này không phải thống kê
handshake TLS upstream và chưa tách theo SNI/certificate.

## Tài nguyên và generation

| Metric | Ý nghĩa |
|---|---|
| `prost_config_generation` | Generation đã publish lúc lấy snapshot |
| `prost_live_generations` | Các published snapshot còn được tham chiếu |
| `prost_snapshot_http_client_pools` | Nhóm ClientPool khác nhau được live snapshots tham chiếu |
| `prost_snapshot_http_worker_clients` | Tổng worker clients trong các nhóm trên |
| `prost_requests_active` | Global HTTP permits đang dùng, gồm lifetime body/tunnel forward |
| `prost_health_probes_active` | Global probe permits đang dùng, tối đa 32 |
| `prost_tasks_tracked` | TaskTracker count, gồm listener, connection, health và các task được đăng ký khác |
| `prost_process_resident_memory_bytes` | Linux `/proc/self/status` VmRSS |
| `prost_process_virtual_memory_bytes` | VmSize, gồm cả vùng địa chỉ chưa resident |
| `prost_process_open_fds` | FD hiện tại, gồm FD dùng đọc thư mục procfs |

Pool gauges đếm đối tượng client được snapshot giữ; **không phải** số socket idle,
pool hit/miss hay mọi pool tạm thời còn được probe/task giữ sau khi snapshot đã
drop. Metrics native chưa can thiệp vào private pool của Hyper để đo các số đó.
Các metric procfs bị bỏ qua nếu không đọc được trên platform hiện tại. RSS không
phải heap live hay cgroup working set. Các family không tạo thành một transaction
snapshot tuyệt đối khi apply/connection đồng thời thay đổi trạng thái.

Danh sách Weak generation được dọn ngay trên commit, cả khi không ai đọc status
hoặc scrape metrics. Các buffer/kết nối của snapshot vẫn chỉ thu hồi sau khi hết
strong reference; metrics không cưỡng bức cắt stream.

## Cấu hình buffer TLS

```toml
[listeners.tls]
certificate = "/etc/prost/server.pem"
private_key = "/etc/prost/server.key"
buffer_limit_bytes = 65536
```

Mặc định 64 KiB giữ hành vi batching trước đây; cho phép 4096–1048576 byte. Có thể
thử 16384 khi cần giảm bộ nhớ trên listener nhiều slow TLS readers, rồi đo lại
throughput/CPU. Giá trị áp dụng cho connection mới, được lưu qua Runtime API.
Nó giới hạn buffering **application data chờ gửi của rustls**, không phải toàn bộ
RAM/connection, receive buffer, handshake hoặc buffer TLS upstream. Nó được đặt
sau khi accept TLS hoàn tất. Certificate/HTTP protocol vẫn hoạt động như trước.

Nguồn: [rustls set_buffer_limit](https://docs.rs/rustls/latest/rustls/struct.ConnectionCommon.html#method.set_buffer_limit).
Quy trình đo: [memory-profiling.md](memory-profiling.md).
Số liệu và các regression đã ghi nhận:
[observability-memory-benchmark.md](observability-memory-benchmark.md).
