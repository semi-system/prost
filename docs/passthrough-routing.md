# Chọn upstream TLS passthrough theo SNI

TCP listener có thể đọc SNI của ClientHello để chọn một upstream đã khai báo,
rồi relay toàn bộ TLS tới backend. Một cổng có thể phục vụ nhiều backend TLS;
Prost không giữ certificate/private key hoặc giải mã application data cho
passthrough. Ví dụ đầy đủ: [tls-passthrough-sni.toml](../examples/tls-passthrough-sni.toml).
Mẫu có default nhận SNI không khớp/thiếu SNI:
[tls-passthrough-default.toml](../examples/tls-passthrough-default.toml).

```toml
[[listeners]]
id = "edge"
address = "0.0.0.0:10443"
protocol = "tcp"
upstream = "web"
tls_passthrough = true

[listeners.tls_passthrough_settings]
reject_unknown_sni = true
client_hello_timeout_ms = 5000
max_client_hello_bytes = 65536

[[listeners.tls_passthrough_settings.routes]]
server_names = ["*.example.test"]
upstream = "web"

[[listeners.tls_passthrough_settings.routes]]
server_names = ["mail.example.test"]
upstream = "mail"
```

`web` và `mail` phải có endpoint đang nói TLS. Không đặt `listeners.tls` hoặc
`upstreams.NAME.tls` cho đường passthrough: client tự handshake/xác minh backend,
còn Prost dùng TCP transport. mTLS cũng đi đầu cuối: client certificate được
backend xác minh, Prost không thay nó bằng identity của proxy.

## Matching và fallback

Tên ASCII DNS được chuẩn hóa chữ thường và bỏ một dấu chấm cuối. IDN dùng
punycode. Exact match luôn thắng wildcard, không phụ thuộc thứ tự khai báo.
`*.example.test` chỉ khớp một nhãn như `api.example.test`, không khớp apex
`example.test` hoặc `deep.api.example.test`. Không hỗ trợ regex, IP literal
hoặc wildcard nằm giữa tên.

Mỗi route cần ít nhất một `server_names` và một upstream hợp lệ. Tối đa 64
route và 256 tên khác nhau cho mỗi listener; route trùng tên sau normalization
bị từ chối, kể cả khi chỉ khác chữ hoa hoặc dấu chấm cuối. Exact/wildcard có
thể cùng tồn tại vì thứ tự ưu tiên đã xác định.

`reject_unknown_sni=true` chấp nhận tên khớp **hợp** của tên trong routes và
`tls_passthrough_settings.server_names`. Danh sách server_names ngoài routes
là allow-only: nếu tên được phép nhưng không có route khớp, dùng upstream mặc
định trên listener. Nó không hạn chế hẹp hơn các tên đã khai báo trong routes.
Strict mode từ chối ClientHello thiếu SNI hoặc không khớp tên nào trước khi
kết nối backend. Chỉ có routes cũng đủ để bật strict mode.

Khi `reject_unknown_sni=false`, tên không khớp hoặc thiếu SNI đi vào upstream
mặc định. ClientHello malformed vẫn bị từ chối. Trường `listeners.upstream`
vẫn bắt buộc theo schema TCP, kể cả khi strict mode khiến default ít được dùng.
Config passthrough cũ không có routes giữ hành vi một upstream trước đây.

Một khi route đã khớp, balancing/health/endpoint enable/disable/capacity/backup
áp dụng trong upstream đó. Nếu không có endpoint khả dụng, phiên thất bại;
không thử upstream mặc định của listener. Backend dự phòng bên trong cùng
upstream vẫn hoạt động theo cấu hình balancing hiện có.

## Hook, PROXY và lifecycle

Connection admission/IP limit/rate/ACL chạy trước khi đọc ClientHello.
SNI routing đặt upstream ban đầu cho event plugin; hook `connection` và
`upstream` có thể thay lựa chọn theo giao diện plugin hiện có. Upstream cuối
cùng vẫn phải tồn tại, không được bọc thêm TLS. Các route tĩnh cũng được kiểm
tra TCP consistent hash chỉ dùng `$remote_addr`.

Ingress PROXY v1/v2 từ peer đáng tin được đọc trước ClientHello. Egress
`send_proxy_v2=true` thuộc upstream đã chọn; header PROXY được gửi **trước**
byte ClientHello tới backend. Client certificate/SNI và byte TLS vẫn giữ nguyên.
Backend cần được cấu hình để đọc PROXY trước TLS khi bật tùy chọn đó.

Parser dùng rustls Acceptor với timeout và byte budget hiện có. ClientHello có
thể bị chia thành nhiều lần đọc; prefix được replay đúng một lần, gồm cả byte
đã đọc sau ClientHello. Không tự terminate TLS, chèn TLS record, sửa SNI hoặc
định tuyến request bên trong TLS. Routing chỉ dùng SNI đọc được trong
ClientHello. STARTTLS không thuộc chức năng
này: đây là TLS bắt đầu ngay khi mở kết nối.

Snapshot được chụp khi accept TCP. Map/SNI policy/upstream đã chọn của phiên
giữ nguyên tới khi relay kết thúc, kể cả đang chờ ClientHello khi config đổi.
Apply thay map cho phiên accept bởi generation mới, tái sử dụng socket lắng nghe.
Mặc định rolling giữ phiên cũ đến khi kết thúc hoặc hết drain deadline;
`reload_mode="hot"` giữ phiên đến khi tự kết thúc. Xem [rolling-updates.md](rolling-updates.md).
Config sai không publish; map được persist cùng config và khôi phục sau restart.
Metrics selection và TCP access log ghi upstream/endpoint thực được chọn.

Health check mặc định vẫn là TCP connect; để kiểm tra TLS thật của backend,
có thể cấu hình TLS riêng dưới `upstreams.NAME.health_check.tls`. Điều này không
bật bọc TLS cho traffic passthrough. Server/client TLS version, cipher và auth
của traffic do hai đầu TLS quyết định; policy terminate TLS của Prost không áp
dụng cho ciphertext đang relay.

## Kiểm chứng local Docker/WSL

168 test đạt ở mỗi chế độ native/all-features; fmt/clippy all-targets/all-features
sạch. Log: `test-results/sni-routing-checks.log`. Một crash fixture được test cha
chạy bằng subprocess, giống các lượt kiểm chứng trước.

Các ca mới kiểm tra normalized duplicate/budget/schema roundtrip, exact so với
wildcard, allow-only/default, strict missing/unknown/apex/deep, ClientHello bị
chia nhỏ và byte sau nó được replay nguyên vẹn. Integration test xác minh TLS
backend, mTLS đầu cuối, PROXY v2/IP/port thật trước TLS, giữ phiên cũ khi đổi map,
route unavailable không fall through, rollback upstream sai/TLS bọc thêm/hash
key HTTP, persist/restart và TLS termination counter bằng 0 tại Prost.

Release native đã kiểm tra:
`sha256:6ce721b6738a7c6e3d57aee2d0fbac526292b5a84ed53d466fa7c0bbfeb244fb`.
`scripts/passthrough-routing-smoke.py` chạy daemon/CLI thật với hai Go TLS
backend, CA và hai leaf certificate khác nhau. Client xác minh hostname/chain
và fingerprint certificate của đúng backend; không dùng verify=false. Kiểm tra
thêm partial ClientHello timeout, hot apply/giữ keepalive, config sai, disable/
enable backend và restart giữ map. Raw:
`test-results/passthrough-routing-final/summary.json` và `commands.json`.
Lượt này chỉ kiểm tra chức năng, không đo throughput hoặc so với Nginx.

```sh
sudo python3 scripts/passthrough-routing-smoke.py --image prost:native \
  --output-dir test-results/passthrough-routing-new
```

Soak đang chạy trong `test-results/soak-24h-edge` dùng immutable image 89da…,
trước cả rate-table API và SNI routing; chưa chứng minh image mới qua 24 giờ.
