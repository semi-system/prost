# Streaming body limits và timeout

Các giới hạn chạy trong data path, không gom toàn bộ body vào RAM và không tạo task riêng cho mỗi timer. Apply bằng Runtime API như các cấu hình khác.

```toml
[upstreams.web]
connect_timeout_ms = 3000
response_header_timeout_ms = 10000
response_body_timeout_ms = 30000
write_timeout_ms = 15000

[[upstreams.web.endpoints]]
id = "app"
address = "127.0.0.1:8081"

[[listeners]]
id = "http"
address = "0.0.0.0:8080"
protocol = "http"
upstream = "web"
header_read_timeout_ms = 10000
client_write_timeout_ms = 15000
max_request_body_bytes = 8388608
request_body_timeout_ms = 15000

[[listeners.routes]]
path_prefix = "/large-upload"
upstream = "web"
max_request_body_bytes = 67108864
request_body_timeout_ms = 30000
```

| Trường | Phạm vi và hiệu lực |
|---|---|
| `header_read_timeout_ms` | Listener HTTP; mặc định 10.000 ms. Bao gồm nhận diện HTTP và headers đầu tiên sau TLS handshake; HTTP/1 còn giới hạn headers của từng request tiếp theo. `0` tắt. |
| `client_write_timeout_ms` | Listener HTTP/TCP; thời gian một thao tác ghi/flush/shutdown socket bị chặn, độc lập với đọc. Mặc định tắt. |
| `max_request_body_bytes` | Listener/route HTTP; giới hạn tổng DATA bytes đã giải mã transfer framing, chưa giải nén Content-Encoding. Mặc định không giới hạn; `0` không giới hạn. |
| `request_body_timeout_ms` | Listener/route HTTP; idle khi chờ frame upload, reset khi có DATA không rỗng. Mặc định tắt; `0` tắt. |
| `response_header_timeout_ms` | Upstream HTTP; khi đặt số dương, bắt đầu sau EOF upload được proxy quan sát. GET/body rỗng bắt đầu ngay. Bao gồm thời gian còn lại cho pool/connect/flush; Hyper không cung cấp sự kiện xác nhận toàn bộ upload đã được ghi xuống socket. `0` tắt deadline headers. |
| `response_body_timeout_ms` | Upstream HTTP; idle khi chờ frame response tiếp theo. Bỏ trống dùng `idle_timeout_ms`; `0` tắt. |
| `write_timeout_ms` | Upstream HTTP/TCP; giới hạn thao tác ghi socket bị chặn. Bỏ trống hoặc `0` tắt. |

Timeout mới nhận giá trị `0..86400000` ms. Route bỏ trống kế thừa listener; route đặt `0` ghi đè thành không giới hạn/tắt. Các trường HTTP body không dùng được trên listener TCP. `connect_timeout_ms` bao gồm DNS, TCP và TLS ở cả HTTP và TCP.

**Tương thích:** nếu không khai báo `response_header_timeout_ms`, giữ cơ chế cũ: `idle_timeout_ms` giới hạn tổng thời gian từ gửi request đến nhận response headers, kể cả upload. Để upload dài mà vẫn giới hạn backend chậm, cần đặt rõ deadline headers và idle upload. Với gRPC hai chiều không giới hạn thời gian upload, có thể đặt headers `0` và chọn idle body phù hợp ứng dụng.

**Kết quả lỗi:**

- Content-Length vượt giới hạn decoder: trả `413` trước khi chọn endpoint. Không dựa vào Content-Length có thể đã bị filter sửa.
- HTTP/1 chunked và HTTP/2 không biết trước độ dài: đếm từng DATA frame, giữ nguyên backpressure và trailers. Không chuyển tiếp frame làm vượt giới hạn. Một phần body trước đó có thể đã đến backend; không thể rollback tác dụng phụ và không retry.
- Idle upload: `408`. Lỗi decoder body được ánh xạ `400` nếu còn có thể gửi response; lỗi framing có thể khiến Hyper đóng connection trước đó. Những lỗi này không tăng bộ đếm lỗi forward/backend và không kích hoạt passive failure.
- Deadline response headers: `504`, tính là lỗi backend. Connect/TLS/write failure giữ mã `503` hiện tại.
- Sau khi response headers đã gửi (ví dụ gRPC/echo hai chiều), lỗi body chỉ có thể hủy stream khi được poll tiếp, không đổi thành status mới. Lease được thu hồi khi kết thúc/hủy.
- Native response headers/trailers tiếp tục áp dụng theo quy tắc `always`/status cho lỗi cục bộ.

**Phạm vi timer:** idle body bắt đầu khi body bị poll và trả Pending; không phải deadline tổng upload/download. Không có timer body khi upstream/downstream chưa yêu cầu poll; một idle đã được kích hoạt vẫn tính thời gian đến lần poll tiếp theo. Socket write timeout đo tình trạng chặn của socket/TLS. Header block và DATA completion deadline riêng của H2 nay có thể bật qua [mvp-bundle.md](mvp-bundle.md), gồm HEADERS/CONTINUATION sau request đầu và DATA bị giữ bởi window. Max connections/requests/streams và flow-control windows vẫn cần đặt phù hợp.

Body policy được giữ theo snapshot của request: upload đang chạy giữ giới hạn cũ; request/HTTP2 stream mới nhận giới hạn mới. Header/socket write settings của listener áp dụng cho connection mới. Đổi upstream socket write timeout tạo pool mới; đổi body/header policy có thể tái sử dụng pool cũ. File sửa trực tiếp không kích hoạt cập nhật.

Kiểm thử trong Docker/WSL gồm H1 Content-Length/chunked/trailers, H2 streaming, hot apply khi upload dở, gRPC kiểu duplex lỗi sau headers, idle upload/download, backend TLS/write bị treo, client không đọc và thu hồi lease. Mặc định không tạo upload state. Timer upload/socket write chỉ khởi tạo khi gặp Pending và được tái sử dụng; deadline headers dùng timer inline trong future riêng của nhánh policy.

HTTP upstream giữ TCP thường trong một enum nhỏ; TLS và wrapper timeout ghi tùy chọn được cấp phát riêng theo connection. Cách này tránh để mọi TCP connection mang kích thước của trạng thái TLS inline trong `MaybeHttpsStream`. ALPN và thông tin địa chỉ socket vẫn được giữ nguyên. Nhánh policy được đặt ngoài future chuyển tiếp mặc định; nếu tắt deadline headers và không cần theo dõi upload, client request đi thẳng vào pool.
