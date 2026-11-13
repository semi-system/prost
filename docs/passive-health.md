# Passive health: ngưỡng lỗi và cooldown

Policy tùy chọn trên mỗi upstream, áp dụng riêng cho từng endpoint:

```toml
[upstreams.web.passive_health]
max_fails = 3
fail_timeout_ms = 10000
failure_statuses = [502, 503, 504]
```

`max_fails` từ 0–1000; `fail_timeout_ms` từ 1–3.600.000 ms. `failure_statuses`
chứa tối đa 32 status 400–599 khác nhau. Trong block, mặc định là `max_fails=1`,
`fail_timeout_ms=10000`, không tính HTTP status. Không khai báo cả block giữ hành
vi MVP: một lỗi forward/connect loại endpoint ngay, probe thành công cho hồi phục
theo `rise`, không có cooldown riêng.

`max_fails=0` tắt **passive** ejection, kể cả các status đã khai báo; active health
vẫn chạy theo `fall`/`rise` như bình thường. Không nên nhầm với disable endpoint.

## Lỗi nào được tính

- TCP: connect/TLS lỗi hoặc timeout trước khi relay bắt đầu. Lỗi client và lỗi
  relay sau connect không tham gia passive policy này.
- HTTP: lỗi trả về từ quá trình forward trước khi có Outcome, gồm connect/TLS,
  chờ headers hoặc đọc frame đầu thất bại. Đây chưa phải phân loại chi tiết mọi
  nguyên nhân: lỗi upload/request body trong forward cũng có thể thuộc nhóm này.
- HTTP status gốc của upstream nằm trong `failure_statuses`, tính một lần khi
  đã nhận headers. Client vẫn nhận nguyên response/body streaming đó; không
  biến nó thành lỗi local hoặc giữ body để thử gửi lại.

Không tính status đã bị response filter thay đổi, rejection của ACL/rate/capacity,
local redirect, CONNECT bị từ chối hoặc client cancellation chỉ drop future.
Không quan sát thêm lỗi response body/trailer sau khi Outcome đã trả về. Không
diễn giải `grpc-status` trong trailers thành HTTP failure; gRPC thường trả HTTP 200.

## Cửa sổ trượt và hồi phục

Mỗi lỗi đủ điều kiện được ghi bằng đồng hồ monotonic. Những lỗi đã cách thời điểm
hiện tại ít nhất `fail_timeout_ms` bị loại khỏi cửa sổ. Đủ `max_fails` thì endpoint
unhealthy trong ít nhất một `fail_timeout_ms` nữa tính từ lỗi chạm ngưỡng.
Response/probe thành công xen giữa không xóa những lỗi chưa hết cửa sổ.

Lỗi của các request còn chạy khi endpoint đang bị cách ly không kéo dài deadline.
Probe vẫn chạy. Hồi phục yêu cầu cả hai điều kiện: cooldown đã hết và một active
probe thành công lúc đó, với bộ đếm `rise` đã đủ. Các probe thành công liên tiếp
trong cooldown vẫn tích lũy `rise`; probe thất bại ngắt chuỗi. Deadline hết mà
không có probe thành công mới thì endpoint vẫn bị loại. Chu kỳ probe và thời
gian chờ capacity có thể kéo dài thời gian quan sát hồi phục so với deadline.
Probe TCP chỉ xác nhận kết nối/TLS, không xác nhận ứng dụng hết lỗi 5xx. Khi cần
kiểm tra ứng dụng, cấu hình thêm `health_check.path` và status mong đợi.

Không tự retry request/byte TCP. Những request trước ngưỡng có thể tiếp tục lỗi
ở primary; sau khi eject, lượt chọn mới dùng các primary còn khỏe hoặc nhóm backup.
Stream/session đã mở vẫn chạy. Ví dụ Compose có primary blue, backup green:
[examples/passive-health.toml](../examples/passive-health.toml).

```sh
prostctl apply --file /examples/passive-health.toml
# Backend demo hỗ trợ /status/400..599, giữ JSON cho cả response lỗi.
curl -i http://127.0.0.1:8080/status/503
curl -i http://127.0.0.1:8080/status/503
curl -i http://127.0.0.1:8080/status/503
prostctl status
curl http://127.0.0.1:8080/   # green trong cooldown; trở lại blue sau probe hồi phục
```

## Hot update và tài nguyên

Cùng endpoint id/address, TLS, HTTP protocol, active policy và passive policy:
generation mới sao chép window và deadline tuyệt đối; thay pool, weight, cap hoặc
enable/disable không kéo dài cooldown. State quan sát giữa các generation là
độc lập: kết quả từ request/probe còn chạy của snapshot cũ không ghi vào state
mới, như cơ chế active health hiện tại. Upstream hoàn toàn không đổi dùng lại Arc.

Đổi riêng passive policy reset window/cooldown; giữ quan sát và bộ đếm active
health để không làm backend đang down theo probe thành healthy. Thay active
policy, address, TLS hoặc HTTP protocol tạo state mới. Policy được lưu bền vững;
window, deadline và health là quan sát runtime, không lưu qua restart. Không
thay đổi `enabled` đã được admin lưu trong config.

Lịch sử chỉ chứa dưới `max_fails` timestamp khi chưa ejected; ejection xóa các
timestamp nhưng có thể giữ capacity đã cấp. Không lưu theo client và không
tăng theo tổng số request. Queue timestamp cấp phát khi có lỗi, tối đa khoảng
16 KiB/endpoint ở ngưỡng 1000 trên target 64 bit, ngoài metadata. Một mutex ngắn
bảo vệ các lỗi cạnh tranh và probe recovery; đường request thành công không
ghi counter, lấy lock, gọi clock hoặc tạo timer của passive window. Có một lookup
bitset khi cấu hình status failures; không tạo task/timer cooldown riêng.

`status.upstreams.<name>[i].passive_health` là `null` cho legacy, hoặc object:

```json
{"enabled":true,"max_fails":3,"fail_timeout_ms":10000,
 "ejected":true,"recent_failures":0,"cooldown_remaining_ms":9500}
```

`recent_failures` là số timestamp còn trong cửa sổ, không phải tổng lỗi lifetime;
bị xóa khi chạm ngưỡng. Remaining bằng 0 có thể vẫn đang chờ probe, nên xem cả
`ejected` và `healthy`. Việc đọc status không tự làm endpoint hồi phục.

## Đối chiếu nginx và kiểm tra

nginx cung cấp `max_fails`/`fail_timeout`; HTTP status nào tính là lỗi còn phụ
thuộc `proxy_next_upstream`. Prost dùng window trượt và active-probe recovery,
không cam kết semantics giống hệt nginx. Nguồn:
[upstream](https://nginx.org/en/docs/http/ngx_http_upstream_module.html),
[proxy_next_upstream](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_next_upstream).
Prost vẫn áp dụng policy khi chỉ có một endpoint; nếu bị eject và không có
backend khác, request mới nhận 503 hoặc TCP đóng kết nối.

Unit tests dùng thời gian ảo cho ranh giới window/cooldown; thêm test lỗi cạnh
tranh, disabled/legacy, validate budgets và state qua apply. Integration kiểm tra
HTTP/2 status/stream, TCP connect, backup, recovery, local 503, CONNECT, invalid
apply và restart. CLI smoke áp dụng config, gây ba 503 rồi kiểm tra backup và
khôi phục policy sau restart.

Các case `rr_passive`/`least_passive` trong benchmark đo tải healthy khi bật
policy trên bốn backend, không đo tốc độ failover. So sánh trước/sau image dùng
workload header không bật policy để phát hiện regression của đường mặc định.
Kết quả và điều kiện đo: [passive-health-benchmark.md](passive-health-benchmark.md).
