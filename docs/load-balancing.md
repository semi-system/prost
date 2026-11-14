# Cân bằng tải và affinity

Mỗi upstream chọn một thuật toán native, dùng chung giữa các I/O worker. Cấu hình
được chuẩn bị trước commit; cập nhật bằng Runtime API không thay worker.
Ví dụ chạy với Compose blue/green: [examples/balancing.toml](../examples/balancing.toml).

| `balance` | Chọn endpoint | Trạng thái trên đường traffic |
|---|---|---|
| `round_robin` (mặc định) | Vé theo trọng số trong các endpoint enabled/healthy | Counter atomic; không theo dõi active load khi chưa bật cap/tracking |
| `least_conn` | Tải đang chạy / weight thấp nhất, hòa thì dùng vé trọng số | Counter active chung, khóa ngắn khi chọn và giữ slot |
| `consistent_hash` | Vòng hash có virtual nodes theo weight | Hash key và binary search; không lưu bảng theo client/key |

Round-robin giữ tỷ lệ trọng số, không cam kết thứ tự giống smooth weighted
round-robin của nginx. Least-conn cho HTTP tính **request/HTTP2 stream đang chạy**,
TCP tính phiên relay; không tính các socket idle trong pool. Đơn vị này có thể
khác số connection vật lý, nhất là khi upstream HTTP/2 multiplex nhiều stream.

## Least-conn

```toml
[upstreams.web]
balance = "least_conn"
[[upstreams.web.endpoints]]
id = "a"
address = "app-a:8080"
weight = 2
[[upstreams.web.endpoints]]
id = "b"
address = "app-b:8080"
weight = 1
```

Giữ tải từ lúc chọn endpoint, gồm thời gian connect, upload, chờ response và
stream body/trailer. WebSocket giữ tới khi tunnel đóng. TCP giữ tới khi relay
kết thúc cả hai chiều, gồm half-close. Lỗi connect, timeout, cancellation và
forced shutdown đều trả lease qua RAII; không tạo task theo dõi riêng.

Tất cả worker và generation tương thích dùng chung counter và khóa chọn. Chỉ
chụp mỗi counter một lần khi chọn; completion có thể đồng thời giảm tải, nên đây
là quan sát tức thời, không phải lịch phân phối tuyệt đối. Khóa chỉ bảo vệ chọn
và tăng counter, không giữ trong lúc I/O.

HTTP dùng một allocation giữ lease trong lifetime body/tunnel khi bật least-conn.
Round-robin và consistent hash chưa bật cap/tracking không trả chi phí theo dõi active load này.

## Consistent hash

```toml
[upstreams.sessions]
balance = "consistent_hash"
hash_key = "$http_x_session"
hash_replicas = 160
[[upstreams.sessions.endpoints]]
id = "a"
address = "app-a:8080"
[[upstreams.sessions.endpoints]]
id = "b"
address = "app-b:8080"
```

| Key | Giá trị |
|---|---|
| `$remote_addr` (mặc định) | IP TCP peer thực, phân biệt IPv4/IPv6; không tin X-Forwarded-For |
| `$uri` | Path sau rewrite, không query |
| `$request_uri` | Path + query nguyên bản trước rewrite |
| `$host` | Host lowercase, bỏ port, cùng quy tắc với HTTP routing |
| `$http_<name>` | Field đầu tiên của header tại lúc chọn upstream, sau request filters; `_` đổi thành `-` |

TCP chỉ dùng `$remote_addr`. Key rỗng/thiếu dùng weighted round-robin; key HTTP
dài hơn 8 KiB nhận 400. Không parse cookie riêng; có thể dùng native headers/map
để trích xuất cookie thành một header nội bộ trước khi hash.

Mỗi endpoint có `weight × hash_replicas` điểm. Key và điểm dùng SHA-256 rút gọn
64 bit; điểm gồm id, address và số replica, không dùng IP DNS đã resolve. Đây là
mapping của Prost, **không tương thích mapping ketama/CRC32 của nginx**.
Ring được sort trước commit, lookup bình thường dùng binary search và kiểm tra
owner. Không tạo map hay cache tăng theo số key.

Reorder endpoint không đổi mapping. Thêm endpoint chỉ chuyển các key sang endpoint
mới; xóa endpoint chỉ chuyển các key trước đó thuộc endpoint bị xóa, khi giữ
nguyên id/address/weight/replicas của các endpoint còn lại. Thay weight thêm/bớt
điểm của endpoint đó. Thay id/address/replicas có thể làm thay đổi mapping nhiều
hơn; đổi hash_key cũng đổi affinity. Restart giữ mapping khi cấu hình không đổi.

Endpoint disable/unhealthy vẫn có điểm trên ring nhưng không nhận request mới;
proxy đi tiếp tới endpoint eligible kế tiếp. Enable/phục hồi đưa các key về owner
ban đầu. Khi mọi endpoint đều không khả dụng, trả lỗi ngay sau kiểm tra tập
endpoint, không quét hết ring. Khi fallback, số bước quét bị chặn bởi kích thước
ring; không có retry HTTP tự động nếu connect/forward tới endpoint đã chọn lỗi.

`hash_replicas` mặc định 160, cho phép 16–1024. Tổng `sum(weight) × replicas`
không vượt **65.536 điểm/upstream**, khoảng 1 MiB dữ liệu điểm trên 64 bit, ngoài
metadata. Vượt budget bị từ chối trước commit. Nếu mọi node có cùng sức mạnh,
dùng weight 1 thay vì những weight lớn bằng nhau; giảm replicas là lựa chọn đổi
độ mịn phân phối lấy RAM. Không tự rescale điểm theo tổng weight, để membership
thay đổi không âm thầm dựng lại toàn bộ vị trí của các node còn lại.

## Hot update và tái sử dụng

- Đã bật tracking, endpoint id/address và TLS/protocol tương thích:
  giữ active counter qua đổi thứ tự, weight, enable/disable, health settings và
  pool settings, cap và thuật toán. Các request/phiên cũ vẫn hiện trong tải của endpoint còn lại.
- Đổi sang least-conn từ thuật toán chưa theo dõi load bắt đầu counter mới;
  request cũ của round-robin/hash không được tính lại hồi tố. Đổi TLS/protocol
  hoặc address tạo counter mới cho tài nguyên không tương thích.
- Ring được dùng lại qua enable/disable, key và transport settings khi thứ tự
  endpoint/id/address/weight cùng cấu hình replicas giữ nguyên. Reorder dựng lại
  cấu trúc chỉ mục nhưng không đổi mapping.
- Pool/TLS được dùng lại khi transport, endpoint identity và các setting client
  tương thích; chỉ đổi thuật toán, weight hoặc enabled không dựng lại pool.
  Đổi địa chỉ/TLS/protocol/connect timeout hoặc pool/window setting tạo pool mới.
- Snapshot cũ vẫn giữ tài nguyên tới khi body/tunnel/session kết thúc. Không
  chuyển request đang chạy sang endpoint khác. Counter active không lưu qua restart.

`prostctl status` bổ sung `balancers.<name>` gồm algorithm, hash_key, hash_points.
`upstreams.<name>[i].active` là active load khi tracking đã bật; null nghĩa là chưa
theo dõi, không phải số 0. Đây là gauge tại thời điểm đọc, không phải RPS.
Backup và giới hạn `max_active`: [upstream-capacity.md](upstream-capacity.md).

```sh
prostctl apply --file ./candidate.toml --if-generation 12
prostctl upstream disable sessions/a --if-generation 13
prostctl upstream enable sessions/a --if-generation 14
prostctl status
```

Retry HTTP bodyless GET/HEAD cùng endpoint có thể bật theo
[mvp-bundle.md](mvp-bundle.md). Chưa có slow-start hoặc tự chuyển session sang
backend khác sau khi đã kết nối. Tham khảo thuật toán của nginx:
[upstream least_conn/hash](https://nginx.org/en/docs/http/ngx_http_upstream_module.html).

Đo bốn backend và đối chứng đường chạy mặc định:
[load-balancing-benchmark.md](load-balancing-benchmark.md).
