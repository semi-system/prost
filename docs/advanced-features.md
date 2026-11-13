# Health check HTTP và cấu hình HTTP/2

## Pool và buffer upstream

```toml
[upstreams.api]
http1_max_buf_size = 65536
pool_max_idle_per_host = 128
pool_idle_timeout_ms = 30000
http2 = false
```

HTTP/1 dùng buffer đọc adaptive, tối đa 64 KiB mặc định; cấu hình từ 8 KiB đến
1 MiB. Đây cũng là giới hạn vùng đọc header upstream, nên cần tăng nếu backend
có response headers lớn. Không gom toàn bộ body vào buffer này.
Pool có timer thu hồi idle sockets tự động, không cần một request mới để dọn.
Thay pool/buffer tạo tài nguyên upstream mới; stream cũ tiếp tục dùng tài nguyên cũ.

`http2=false` khóa HTTP/1.1 cho upstream, kể cả TLS server hỗ trợ ALPN h2.
`http2=true` dùng HTTP/2, với ALPN h2 trên TLS và prior knowledge trên TCP.
Đổi protocol có thể apply trên frontend keep-alive mà không restart listener.

## Health check chủ động

Mặc định vẫn probe TCP/TLS. Để phát hiện ứng dụng lỗi dù port còn mở:

```toml
[upstreams.api]
health_interval_ms = 1000

[upstreams.api.health_check]
path = "/ready"
host = "api.internal"
expected_status = 200
timeout_ms = 800
fall = 3
rise = 2
```

`path` không khai báo sẽ dùng TCP/TLS. Khi có `path`, probe GET và đối chiếu đúng
status, không theo redirect và không kiểm tra nội dung body. `host` mặc định là
địa chỉ endpoint. TLS upstream vẫn xác minh certificate và server name theo
`upstreams.api.tls`; không có tùy chọn bỏ verification. `http2=true` dùng probe
HTTP/2, còn lại dùng HTTP/1.1.

`timeout_ms` bao trùm connect, TLS, handshake HTTP và response headers. `fall` và
`rise` yêu cầu số kết quả liên tiếp, từ 1 đến 100. Endpoint khởi tạo healthy để
giữ tương thích hành vi MVP; probe đầu chạy ngay. Endpoint bị disable không
được probe và không nhận traffic mới. Passive transport failure vẫn eject ngay,
không chờ `fall`; HTTP 5xx của request thường không tự eject.
Đây là mặc định khi không có passive policy. Có thể cấu hình ngưỡng lỗi/cooldown
và danh sách HTTP status: [passive-health.md](passive-health.md).

Probe chạy ngoài I/O workers, tối đa 32 đang thực thi toàn proxy và 16 task cho
mỗi upstream. Chu kỳ là thời gian chờ sau khi hoàn thành một lượt, không phải
lịch fixed-rate. Khi timeout/shutdown, HTTP driver của probe cũng bị hủy. Không
tự retry traffic khi probe fail; client nhận 503 nếu không còn endpoint khỏe.

Runtime API `status` bổ sung:

```json
{"upstreams":{"api":[{"id":"server-1","enabled":true,"healthy":false}]}}
```

Thay policy hoặc path có hiệu lực qua transaction `apply`. Nếu endpoint có cùng
id/address, TLS, HTTP protocol và policy thì cấu hình mới sao chép health state;
hai generation không dùng chung bộ đếm probe. Upstream hoàn toàn không đổi vẫn
tái sử dụng pool/state như trước. State health là trạng thái quan sát, không lưu
xuống TOML; `enabled` vẫn được lưu bền vững.

## Flow control HTTP/2

Cấu hình độc lập ở listener và upstream:

```toml
[listeners.http2_settings]
initial_stream_window_size = 1048576
initial_connection_window_size = 8388608
max_concurrent_streams = 128
adaptive_window = false

[upstreams.api.http2_settings]
initial_stream_window_size = 1048576
initial_connection_window_size = 8388608
max_concurrent_streams = 128
adaptive_window = false
```

Window mặc định: stream 65.535 byte, connection 1 MiB; mỗi window cho phép từ
65.535 byte đến 16 MiB. `max_concurrent_streams` mặc định 128, giới hạn 1–4096.
Ở listener đây là giới hạn incoming request streams. Ở upstream nó cấu hình
giới hạn stream mà HTTP/2 client chấp nhận từ peer; số request stream outbound
còn do SETTINGS của backend quyết định. Giới hạn request toàn proxy vẫn dùng
`runtime.max_requests` và không biến thành hàng đợi không giới hạn.

Window là lượng data peer có thể gửi trước khi cần WINDOW_UPDATE, không phải
body buffer được cấp phát sẵn. Window lớn cho phép nhiều dữ liệu đang truyền
hơn và có thể tăng bộ nhớ. `adaptive_window=true` để Hyper điều chỉnh window;
khi đó các initial window không cố định hành vi cuối cùng.

Listener settings mới chỉ áp dụng cho connection mới; stream trên HTTP/2
connection cũ vẫn có flow control đã thương lượng. Routing/plugins của request
mới vẫn nhận snapshot mới. Đổi settings upstream tạo client pool mới, stream
cũ giữ generation và pool cũ đến khi hoàn tất.

API tham khảo:
[Hyper server HTTP/2 builder](https://docs.rs/hyper-util/latest/hyper_util/server/conn/auto/struct.Http2Builder.html),
[Hyper client builder](https://docs.rs/hyper-util/latest/hyper_util/client/legacy/struct.Builder.html).
