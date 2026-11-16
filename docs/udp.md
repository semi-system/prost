# UDP reverse proxy

Listener `protocol="udp"` chuyển tiếp datagram IPv4/IPv6 tới upstream bằng một
socket UDP connected riêng cho mỗi phiên. Không tách/gộp datagram, kể cả payload
rỗng. Socket backend chỉ nhận packet từ endpoint đã chọn. Không tự retry datagram.

Ví dụ: [udp.toml](../examples/udp.toml).

```toml
schema_version = 1
[runtime]
max_udp_buffer_bytes = 67108864 # 64 MiB chung cho response buffer + queue

[upstreams.dns]
[upstreams.dns.health_check]
enabled = false
[[upstreams.dns.endpoints]]
id = "resolver"
address = "127.0.0.1:5353"

[[listeners]]
id = "dns-udp"
address = "0.0.0.0:1053"
protocol = "udp"
upstream = "dns"
max_connections = 1024
max_connections_per_ip = 32
[listeners.udp]
idle_timeout_ms = 30000
max_datagram_bytes = 4096
session_queue_packets = 8
session_requests = 1
session_responses = 1
```

TCP/HTTP và UDP có thể dùng cùng IP/port nếu listener ID khác nhau.
UDP không có TLS/DTLS, PROXY header, HTTP route hay các plugin TCP/HTTP.
UDP upstream không dùng HTTP policy, HTTP/2, TCP retry hoặc TLS. Health check
TCP mặc định phải tắt bằng `health_check.enabled=false`: cổng UDP không được
đánh giá bằng việc thử mở kết nối TCP. Chưa có active UDP payload/match probe.

## Phiên và load balancing

Ánh xạ dùng source IP + source port của client, tách riêng theo listener.
Weighted round-robin, least-conn, consistent hash theo `$remote_addr`, endpoint
backup/disable và `max_active` được áp dụng khi tạo phiên. Consistent hash không
nhận HTTP/header/path key trên UDP.

`session_requests=0` giữ mapping tới idle timeout; giá trị dương giới hạn số
datagram client trên một mapping. Khi đã nhận đủ, datagram tiếp theo tạo phiên
mới, trong khi phiên cũ vẫn có thể chuyển nốt response. `session_responses=0`
không giới hạn số response; giá trị dương kết thúc phiên sau số response đó.
Đặt cả hai bằng 1 cho giao thức query/response đơn như DNS không có multi-reply.
Các giá trị là số datagram, không phân tích transaction ID của ứng dụng.

Idle timeout tính lại khi chuyển thành công dữ liệu ở một trong hai hướng.
Backend không trả lời thì hết timeout sẽ nhả socket, lease, mapping và buffer.
DNS hệ thống hoặc resolver TTL/SRV hiện có được dùng cho phiên mới; phiên đang
chạy giữ endpoint đã resolve. Không dùng lỗi UDP để đánh dấu passive TCP health
vĩnh viễn khi active probe đã tắt.

## Rolling update và giới hạn tài nguyên

Listener cùng ID/address/protocol tái sử dụng socket và bảng peer qua rolling.
Peer cũ giữ upstream/snapshot cũ, peer mới dùng generation mới. Khi idle hết,
peer cũ tạo phiên tiếp theo theo config mới. Worker generation cũ vẫn chịu drain
deadline hoặc `drain_seconds=0`; không tự đóng phiên chỉ để xoay certificate.
Đổi protocol của một listener ID cần restart; thêm/bind UDP listener mới thất
bại thì không commit candidate. Restart khôi phục config, không khôi phục phiên.

Xóa listener dừng nhận thêm datagram trên địa chỉ đó; response backend đang chờ
có thể drain. Giới hạn kích thước datagram phía nhận dùng cấu hình listener
hiện hành; phiên cũ vẫn dùng buffer/backend policy của generation cũ.

Giới hạn toàn proxy/listener/theo IP đếm phiên UDP tương tự connection TCP.
Queue mỗi phiên có trần số packet, và từng packet cùng response buffer giữ
byte permit từ `max_udp_buffer_bytes`. Hết budget/queue/session capacity sẽ drop
datagram có counter; không dựng queue chờ không giới hạn. Datagram quá lớn bị
drop nguyên gói, không chuyển payload đã truncate. Mỗi dispatcher có thêm một
receive buffer cố định 64 KiB; budget không phải trần RSS hay kernel socket RAM.

## Quan sát

Session log JSON dùng `protocol="udp"`, ghi endpoint, byte và lý do hoàn tất.
`prostctl status` có `udp`: budget, reservation và số peer mapping theo listener.
Mapping có thể ít hơn số phiên đang sống khi query đã gửi đủ request nhưng còn
chờ response; `connections` vẫn đếm các phiên đó đến lúc kết thúc.

Prometheus:

- `prost_udp_datagrams_total{direction="client|backend"}`;
- `prost_udp_dropped_total{reason="oversize|buffer_capacity|queue_capacity|session_capacity|session_closed|io"}`;
- `prost_udp_buffer_reserved_bytes`, `prost_udp_buffer_budget_bytes`.

Kiểm thử:

```sh
sh scripts/docker-dev.sh cargo test -p prost-core --test proxy udp_ --locked
```
