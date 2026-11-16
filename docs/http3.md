# HTTP/3 over QUIC

`protocol = "http3"` mở listener UDP dùng QUIC/TLS 1.3 và ALPN `h3`.
Request đi trực tiếp qua pipeline HTTP hiện có: routing host/path/method/header,
ACL, native headers/rewrite, rate limit, proxy RAM cache, compression, static
files, body limits, access log và HTTP/1.1 hoặc HTTP/2 upstream. Không có bước
chuyển tiếp qua một listener HTTP nội bộ.

Ví dụ: [http3.toml](../examples/http3.toml). Listener HTTPS TCP và HTTP/3 UDP có
thể dùng cùng số cổng. Cần mở/publish **cả TCP và UDP** ở firewall/container.
Header `Alt-Svc` được cấu hình bằng native header trên listener HTTPS; Prost
chưa tự quảng bá hoặc tự thêm listener dự phòng.

```toml
[runtime]
max_quic_receive_bytes = 67108864 # 64 MiB toàn proxy, đổi cần restart

[[listeners]]
id = "quic"
address = "0.0.0.0:443"
protocol = "http3"
upstream = "web"
header_read_timeout_ms = 10000

[listeners.tls]
certificate = "/run/certs/fullchain.pem"
private_key = "/run/certs/key.pem"

[listeners.http3]
max_connections = 64
max_streams = 32
connection_receive_window = 1048576
stream_receive_window = 65536
send_window = 1048576
idle_timeout_ms = 60000
stream_write_timeout_ms = 30000
```

Giới hạn connection chung/listener/theo IP và budget handshake vẫn áp dụng.
Mỗi QUIC connection giữ reservation bằng `connection_receive_window`; thiếu
budget thì từ chối connection mới. Đây là budget receive window, **không phải**
giới hạn RSS toàn tiến trình; send buffer, TLS, stream metadata và ứng dụng còn
cần headroom. Datagram ứng dụng và migration bị tắt. Giới hạn stream tính cả
response đã FIN nhưng còn chờ ACK; không tạo hàng đợi delivery vô hạn.

Header section tối đa 32 KiB. `header_read_timeout_ms` tính riêng cho mỗi stream.
`client_write_timeout_ms`, nếu có, ghi đè `stream_write_timeout_ms`; timeout này
giới hạn từng lần ghi bị chặn bởi flow control, không tính thời gian chờ chunk
từ upstream. Hủy stream bằng STOP_SENDING giải phóng request/body đang chờ
origin; không cần đóng cả connection. Upload/download và trailers đều streaming
với backpressure; queue chuyển body vào pipeline chứa tối đa một frame 16 KiB.

TLS dùng cùng kho artifact, SNI strict/default, mTLS và chính sách cipher với
listener TCP. QUIC chỉ thương lượng TLS 1.3 và cần TLS_AES_128_GCM_SHA256 cho
Initial; policy không tương thích bị từ chối trước commit. Không bật 0-RTT.
PROXY protocol đầu vào, TLS passthrough, WebTransport, extended CONNECT và
upstream HTTP/3 chưa hỗ trợ. Không dùng listener này cho WebSocket; dùng
listener HTTP/1.1 cho Upgrade.

Full rolling update giữ endpoint/socket và QUIC connection table. Connection
mới dùng config/cert mới; stream đang chạy giữ generation cũ. Generation cũ gửi
GOAWAY, drain các stream đang chạy, chờ FIN được xác nhận rồi đóng connection;
deadline/force của rolling và shutdown vẫn áp dụng. TLS-only reload thay cert
cho handshake mới mà không drain connection hiện hành. Hot mode giữ hành vi
snapshot theo request như HTTP/2.

Prometheus có `prost_quic_receive_reserved_bytes` và
`prost_quic_receive_budget_bytes`; traffic/request/TLS và worker generation dùng
các metric chung. Access log ghi HTTP version `HTTP/3.0`.

```sh
sh scripts/docker-dev.sh cargo test -p prost-core --test proxy http3_ --locked
```

Kiểm thử dùng client QUIC thật: strict SNI, Host ACL, native headers,
gRPC/trailers, upload hai chiều trước EOF, body limit, cache hit khi backend bị
disable, stream cancellation, header/write timeout, static RAM và TCP/UDP cùng
cổng, xoay cert, bind rollback, rolling giữ stream và restart. Chưa có soak
HTTP/3 dài hoặc benchmark để kết luận chất lượng production/hiệu năng.
