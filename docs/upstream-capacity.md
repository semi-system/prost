# Backend dự phòng và giới hạn tải

`backup` và `max_active` nằm trên từng endpoint. Có thể apply qua Unix socket,
được lưu vào managed TOML và khôi phục sau restart. Ví dụ chạy với blue/green:
[upstream-capacity.toml](../examples/upstream-capacity.toml).

```toml
[upstreams.web]
balance = "least_conn"
[[upstreams.web.endpoints]]
id = "primary"
address = "app-a:8080"
max_active = 32
[[upstreams.web.endpoints]]
id = "reserve"
address = "app-b:8080"
backup = true
max_active = 128
```

## Chọn backend

Proxy xét endpoint enabled, healthy và còn sức chứa. Nếu có ít nhất một primary
đủ điều kiện, toàn bộ backup bị loại khỏi lượt chọn. Khi mọi primary đều down,
disabled hoặc đầy, proxy chọn trong nhóm backup theo cùng thuật toán và weight.
Primary có chỗ trở lại được ưu tiên ngay cho request/phiên mới. Chỉ có backup
trong cấu hình cũng hợp lệ; nhóm đó hoạt động như tầng cuối.

`backup = false` là mặc định. Backup dùng được với `round_robin` và `least_conn`;
`consistent_hash` có backup bị từ chối trước commit. Hash vẫn hỗ trợ `max_active`:
khi owner đầy, đi tiếp trên ring tới endpoint còn chỗ; khi owner hết đầy, key lại
trở về owner. Không thay đổi ring hay tạo bảng lưu theo key để làm việc này.

Không retry request đang forward. Connect lỗi làm request đó thất bại và ghi nhận
passive failure; các lượt chọn tiếp theo mới dùng tập endpoint khả dụng mới.
Session/TCP stream đã kết nối không tự chuyển backend. Chưa có slow-start.

## Sức chứa và vòng đời

`max_active = 0` (mặc định) không giới hạn tải trên endpoint. Giá trị dương tính:

- HTTP: một request/HTTP2 stream đang forward, gồm connect, upload, chờ header,
  response body và trailers. WebSocket giữ slot tới khi tunnel đóng.
- TCP: một phiên relay đang chạy, tới khi cả hai chiều kết thúc; half-close
  một chiều chưa tự trả slot khi chiều còn lại vẫn chạy.

Không tính socket idle trong pool hoặc health probe. Với HTTP/2 upstream, nhiều
request trên cùng một socket vẫn tính nhiều slot. Giới hạn dùng chung giữa I/O
worker và generation có trạng thái tương thích, trong một daemon.

Một khóa ngắn bảo vệ chọn endpoint và tăng counter cùng nhau. Không chờ slot
và không giữ khóa trong I/O; completion trả slot bằng atomic RAII. Khi toàn bộ
endpoint đều không khả dụng, HTTP trả **503**, TCP đóng kết nối mới. HTTP response
filters có `always` vẫn chạy trên 503. Đầy sức chứa không đánh dấu backend unhealthy.
Đây là admission trước forward, không phải cơ chế loại bỏ TCP backlog hoặc mọi
buffer/queue ở tầng protocol; vẫn cần đặt giới hạn connection/request của runtime.

Counter và lease chỉ bật khi dùng least-conn, bật ít nhất một `max_active`, hoặc
đang giữ accounting từ generation trước. Round-robin/hash chưa bật tracking không
có allocation lease hay atomic tăng/giảm trên từng request. Khi bật tracking,
HTTP dùng một allocation theo lifetime body/tunnel, giống cơ chế của least-conn.
Không sinh task/queue/timer cho từng slot và không lưu state theo client.

## Hot update

- Đổi weight, backup, enabled, cap, thuật toán hoặc pool settings giữ counter khi
  endpoint id/address và TLS/protocol vẫn tương thích. Reorder không mất counter.
  Pool vẫn được dùng lại khi các setting transport/client tương thích.
- Hạ cap xuống dưới active hiện tại giữ stream/session cũ, chặn admission mới
  tới khi active thấp hơn cap. Tăng cap có hiệu lực với lượt chọn dùng config mới.
- Bỏ cap hoặc chuyển khỏi least-conn vẫn giữ accounting cho các generation tương
  thích sau đó, để bật lại cap không làm mất tải đang chạy. Restart mới bỏ tracking
  nếu config không còn cần; counter không lưu xuống đĩa.
- Bật cap lần đầu trên round-robin/hash chưa tracking bắt đầu đếm từ generation
  mới. Phiên cũ chưa có lease không được tính hồi tố. Muốn cap bao phủ mọi phiên
  ngay khi khởi chạy, cấu hình nó từ startup; khi đổi nóng cần để phiên cũ drain.
- Đổi address/TLS/protocol hoặc xóa rồi thêm lại endpoint tạo miền tài nguyên mới;
  counter mới không tính phiên còn sống của miền cũ. Request đã lấy snapshot cũ
  tiếp tục dùng policy cũ; cập nhật không thu hồi admission đang thực hiện.

`prostctl status` trả thêm `backup`, `max_active`, `active` trên mỗi endpoint.
`active: null` nghĩa là chưa theo dõi tải, không có nghĩa là zero. Export và
enable/disable qua Runtime API giữ nguyên backup/cap đã khai báo.

## Đối chiếu nginx

nginx định nghĩa `max_conns` theo **active connection vật lý** và cần shared zone
để giới hạn chung giữa các worker; `backup` không dùng cùng hash. Prost dùng tên
`max_active` để thể hiện đơn vị request/stream, có thể khác đáng kể khi multiplex
HTTP/2 upstream. Xem [tài liệu upstream của nginx](https://nginx.org/en/docs/http/ngx_http_upstream_module.html).

Benchmark thêm trong `scripts/balance-bench.py --cases rr_backup least_backup`:
một primary chậm 5 ms có cap 16, ba backup không giới hạn riêng; nginx dùng zone,
`max_conns=16`, HTTP/1.1 upstream. Preflight tuần tự phải vào primary, còn tải
128 stream phải thực sự đi vào cả primary và backup. Counters của cả bốn backend
phải cộng đúng số request, không cache/retry. Các phép đo HTTP/1 upstream nhỏ này
không chứng minh hai loại cap tương đương trên HTTP/2 upstream.

Kết quả và đối chứng trước/sau khi thêm tính năng:
[upstream-capacity-benchmark.md](upstream-capacity-benchmark.md).
