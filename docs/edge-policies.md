# HTTP policy và TCP connection policy

Đây là các tính năng chung của Prost, không phụ thuộc Rusty hay cấu hình dịch vụ
cụ thể. Ví dụ: [edge.toml](../examples/edge.toml). Các tùy chọn mới tắt mặc định,
không thêm scanner/header clock hoặc retry vào đường xử lý không sử dụng chúng.

## Native Host ACL

```toml
[[listeners.plugins]]
name = "host_acl"
config = { hosts = ["example.test", "api.example.test"], status = 421 }
```

Cho phép chính xác các hostname/IP trong danh sách (1–256), không có DNS lookup
hoặc wildcard. So sánh DNS không phân biệt hoa thường, bỏ một dấu chấm cuối;
IP được canonicalize. Port không tham gia allowlist; cấu hình host không có port.
Thiếu Host/authority, Host trùng lặp, userinfo/port không hợp lệ bị từ chối.
HTTP/2 dùng :authority; nếu có cả authority và Host thì chúng phải khớp host và
port (chuẩn hóa default port theo scheme). Không lấy Host từ forwarded headers.

Status mặc định 421, có thể chọn HTTP 400–599. Đặt host_acl trước rewrite/redirect
để chặn host lạ trước khi tạo redirect; response cục bộ vẫn qua native response
headers. Không mở kết nối upstream khi bị ACL từ chối. Có thể dùng cùng listener
chỉ redirect, không cần upstream. Filter được compile trước apply; request mới
trên keepalive/H2 connection sử dụng ACL mới.

## HTTP/1 keepalive idle riêng với header timeout

```toml
[[listeners]]
id = "http"
address = "0.0.0.0:8080"
protocol = "http"
upstream = "web"
header_read_timeout_ms = 10000
keepalive_idle_timeout_ms = 30000
```

Không khai báo hoặc 0 giữ hành vi cũ của Hyper. Khi bật, timeout chờ request tiếp
theo tách khỏi budget đọc header; request bắt đầu gửi thì chuyển sang header
budget. Đọc header đầu tiên vẫn dùng header_read_timeout_ms. Không dùng idle
budget để cắt upload, chờ upstream, body streaming hoặc socket đang backpressure.
Khi idle hết hạn, graceful close, không gửi một HTTP 408 không được yêu cầu.
Header gửi dở vẫn có thể nhận timeout của Hyper. Header timeout 0 tắt budget
header, nhưng idle giữa các request vẫn được giới hạn nếu cấu hình idle >0.

Tùy chọn này chỉ quản lý HTTP/1. HTTP/2 dùng giới hạn stream/header/write hiện có;
WebSocket sau upgrade giữ timeout tunnel/relay. Giá trị được chụp khi accept
connection; apply ảnh hưởng connection mới. ACL/routing vẫn cập nhật từng request.

Adapter dùng public Hyper Timer và IO interfaces, không fork Hyper. Một observer
framing chỉ khi bật idle timeout phân biệt prefix đã pipeline trong buffer,
Content-Length, chunk-size/extensions và trailers; không buffer body. Hyper vẫn
quyết định request có hợp lệ hay không. Framing không chắc chắn fallback về header
budget, không tự kéo dài sang idle budget. Header/trailer scratch tối đa 32 KiB,
chunk-size line tối đa 1 KiB; không mở một task/timer riêng cho mỗi request.

Adapter dựa vào cách Hyper 1.12 gọi HTTP/1 header Timer. Cargo.lock khóa phiên bản;
khi nâng Hyper cần chạy lại các ca pipelining/chunked/slow reader/idle. Tham khảo
[public HTTP/1 builder](https://docs.rs/hyper/1.12.0/hyper/server/conn/http1/struct.Builder.html).

## TCP retry trước application traffic

```toml
[upstreams.stream.tcp_retry]
max_attempts = 3
timeout_ms = 9000
backoff_ms = 25
```

Mặc định không retry. max_attempts (1–8) gồm lần đầu; timeout_ms (1–60000) là
budget tổng, backoff_ms 0–1000. Mỗi lần dial vẫn chịu connect_timeout_ms của
upstream; backoff cũng tính vào budget tổng. Không queue chờ slot bổ sung.

Chỉ retry lỗi DNS/TCP dial có thể phục hồi, trên cùng endpoint đã chọn. Không
retry lỗi permission/invalid input, ghi PROXY header, TLS verification/handshake,
hoặc bất kỳ lỗi nào sau khi kết nối đã sẵn sàng relay. Không đọc/buffer/replay
application bytes để retry, không tự chuyển sang endpoint khác trong cùng phiên.
Health check và HTTP pool không sử dụng policy TCP này.

Admission/endpoint lease giữ xuyên suốt các lần thử; chỉ lỗi cuối cùng mới ghi
passive failure. Session mới vẫn qua balancing/health như bình thường. Metric
`prost_tcp_connect_retries_total{upstream,endpoint}` đếm lần thử bổ sung, không
phải tổng số kết nối; hot apply tương thích giữ counter. Policy mới áp dụng cho
TCP session mới, phiên đang relay không bị đổi hoặc phát lại.

## Connection window có giới hạn tài nguyên

```toml
[[listeners.plugins]]
name = "connection_window"
config = { max_connections = 100, window_ms = 10000, max_keys = 4096, expire_ms = 30000 }
```

Đếm theo IP socket hoặc trusted ingress PROXY; IPv4-mapped IPv6 dùng chung key
với IPv4. Áp dụng ở hook connection, trước TLS, cho TCP và HTTP; không đếm HTTP2
streams. Config không đổi giữ state qua apply của route/upstream. Thứ tự với các
filter khác theo khai báo; global/per-IP active cap có thể từ chối trước hook.

Dùng hai bucket thời gian có trọng số: current + phần previous chưa hết window.
Đây là xấp xỉ sliding window, không phải lưu mọi timestamp và không cam kết khớp
stick-table HAProxy từng thời điểm. Lần bị chính window từ chối cũng được đếm;
burst không tự reset hoàn toàn ngay ở ranh giới period. GCRA connection_rate
hiện có vẫn là lựa chọn riêng cho token/burst smoothing.

window_ms 1–3600000, max_connections 1–1000000, max_keys 1–100000. Memory mỗi key
cố định, không tỷ lệ với số attempt trong window. Hash table chia shard với quota
riêng; shard đầy từ chối ngay, không tăng memory hoặc queue dù shard khác còn
chỗ. expire_ms mặc định max(2×window,30000), tối đa một ngày và không được nhỏ hơn
hai window, tránh eviction làm reset một cửa sổ còn hiệu lực. Cleanup lazy có
giới hạn theo shard; không polling cấu hình. Không persist rate state qua restart.

## Kiểm chứng

Xem/xóa state của `connection_window`, `connection_rate` và `rate_limit` qua
Unix socket, có kiểm tra generation: [rate-tables.md](rate-tables.md).

Các ca mới kiểm tra Host/authority, thứ tự ACL/redirect và apply trên H1/H2;
keepalive dài hơn header deadline, quiet close, pipeline prefix, streaming/H2;
TCP dial phục hồi và payload đúng một lần, exhaustion/total budget, không retry
sau PROXY/TLS; window concurrency, ranh giới/expiry/capacity và hot apply.
Kết quả release và giới hạn đo được ghi trong [edge-policies-results.md](edge-policies-results.md).
