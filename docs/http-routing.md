# Routing HTTP theo method và header

Route có thể kết hợp host, path prefix, method và header của request đầu vào.
Ví dụ đầy đủ: [http-routing.toml](../examples/http-routing.toml).

```toml
[[listeners.routes]]
host = "api.example.test"
path_prefix = "/api/"
priority = 20
methods = ["GET", "HEAD"]
headers = [{ name = "x-release", equals = "canary", ignore_case = true }]
upstream = "api"
```

Methods trong danh sách kết hợp **OR**. Host/path/method và mọi điều kiện header
kết hợp **AND**. Bỏ methods hoặc dùng `[]` để nhận mọi method; bỏ headers để
không lọc header. Method phân biệt hoa/thường và hỗ trợ custom token: `GET`
khác `get`; route GET không tự nhận HEAD. Header name không phân biệt hoa/thường.
HTTP/2 `:method` dùng trường methods; `:authority` dùng host, không khai báo
pseudo-header trong headers.

## So sánh header

Mỗi phần tử trong headers bắt buộc có name và đúng một phép so sánh:

| Trường | Hành vi |
|---|---|
| equals = "value" | Có một occurrence bằng literal |
| not_equals = "value" | Header tồn tại và không occurrence nào bằng literal |
| prefix = "value" | Có một occurrence bắt đầu bằng literal |
| matches = "pattern" | Có một occurrence khớp regex |
| not_matches = "pattern" | Header tồn tại và không occurrence nào khớp regex |
| present = true/false | Kiểm tra có/không header, kể cả giá trị rỗng |

ignore_case mặc định false, dùng ASCII case-insensitive cho literal/prefix và
byte regex; không dùng với presence. Regex là byte regex của thư viện Rust regex,
được compile khi chuẩn bị config. Không có bước decode UTF-8 header để match;
header chứa byte ngoài UTF-8 vẫn được xử lý. Regex search không tự anchor;
dùng `^...$` khi cần khớp cả giá trị.

```toml
[[listeners.routes]]
path_prefix = "/events/"
methods = ["POST"]
upstream = "api"
[[listeners.routes.headers]]
name = "content-type"
prefix = "application/json"
[[listeners.routes.headers]]
name = "x-tenant"
matches = "^tenant-[0-9]+$"
[[listeners.routes.headers]]
name = "x-disabled"
present = false
```

So sánh giá trị không trim/split comma-list; dùng các occurrence do HTTP parser
trả về. Hai điều kiện cùng header có thể được thỏa bởi hai occurrence khác
nhau. Header thiếu không thỏa equals/prefix/regex hoặc các phép not_; dùng
present=false để chọn trường hợp thiếu. Giá trị rỗng vẫn là header có mặt;
equals="" chỉ khớp occurrence rỗng.

## Thứ tự, fallback và hot update

Thứ tự route giữ nguyên: priority giảm dần, prefix dài hơn, rồi thứ tự khai báo.
Method/header không tự tăng độ ưu tiên. Candidate không khớp predicate được bỏ
qua để thử route tiếp theo, kể cả khi nhiều route có cùng prefix. Nếu không có
route phù hợp, dùng upstream mặc định của listener; nếu listener không có default
thì trả lỗi theo hành vi hiện tại. Một route đã khớp nhưng upstream không khả
dụng không fall through sang route/default khác; balancing/backup bên trong
upstream vẫn áp dụng.

Matching chạy trên method/header/URI **trước** native header/rewrite filters.
Filter sửa header hoặc URI không kích hoạt chọn route lại. Route đã chọn quyết
định upstream, plugin, access-log override và body limit. Cả nhánh route nhỏ và
route index đều dùng cùng predicate; bật access log không làm đổi kết quả.

Apply compile matcher trước persistence/publish. Regex sai hoặc vượt budget,
schema sai, header/method không hợp lệ giữ config và generation cũ. Request đang
upload/download giữ snapshot/route/body limit đã chụp; request hoặc H2 stream mới
trên connection hiện có nhận matcher mới sau commit khi `reload_mode="hot"`.
Mặc định rolling giữ snapshot theo connection và drain thế hệ cũ; client mở
connection mới để nhận matcher mới: [rolling-updates.md](rolling-updates.md).
Config persist/restart giữ
method/header rules. Route plugin state giữ lại khi identity host/path/priority/
method/header không đổi; đổi matcher được xem là thay identity route, tương tự
đổi host/path trước đây.

Tối đa 32 method/route (token 1–64 byte), 16 header predicate/route, header name
128 byte, literal 4096 byte, regex pattern 2048 byte. Toàn config tối đa 256
route-header regex; mỗi regex có compiled size limit 64 KiB, DFA cache limit
64 KiB và nesting limit 32. Giới hạn config/route tổng thể hiện có vẫn áp dụng.
`prost check` kiểm tra schema; `prostctl validate` còn compile regex/budget mà
không commit.

## Default cho SNI và HTTP

SNI thuộc ClientHello TLS. Fallback của TLS passthrough là **upstream trên
listener**, không phải HTTP response header. Cấu hình bắt các SNI không khớp
hoặc thiếu SNI: [tls-passthrough-default.toml](../examples/tls-passthrough-default.toml).

```toml
[[listeners]]
id = "edge"
address = "0.0.0.0:10443"
protocol = "tcp"
upstream = "default"
tls_passthrough = true
[listeners.tls_passthrough_settings]
reject_unknown_sni = false
```

Khi strict=true, unknown/missing SNI bị từ chối; tên allow-only được khai báo
riêng vẫn có thể dùng default. Route SNI đã khớp nhưng không còn backend khả
dụng không tự chuyển sang default. Default backend giữ certificate/key và client
vẫn xác minh hostname/CA như bình thường: fallback không tạo certificate cho
mọi tên miền lạ. Chi tiết: [passthrough-routing.md](passthrough-routing.md).

Với TLS termination, certificate/private_key trực tiếp trong listeners.tls là
certificate mặc định nếu reject_unknown_sni=false; xem [tls-sni.md](tls-sni.md).
Sau handshake, HTTP host/path/method/header routing chạy độc lập. Header response
muốn thêm vẫn dùng native headers plugin như trước.

## Kiểm chứng local Docker/WSL

Toàn bộ workspace đạt **174 test ở mỗi chế độ native/all-features**;
fmt/clippy all-targets/all-features sạch. Raw: `test-results/http-routing-checks.log`.
Các ca mới bao phủ:

- Schema/operator/token, regex syntax/compiled budget và tổng số regex.
- Method case-sensitive/custom/HEAD, header duplicate/presence/byte không UTF-8,
  negative comparison và AND/OR semantics.
- Route nhỏ và route index, priority/prefix/declaration order, host/authority,
  HTTP/1 keepalive và H2 stream trên kết nối đang mở.
- Headers match trước native rewrite; nhánh có/không access log.
- Upload được gửi tới backend trước khi body hoàn tất; hot apply giữ route/body
  limit cũ cho upload đang chạy và áp dụng limit/upstream mới cho stream mới.
- Validate/apply lỗi giữ generation/file cũ, restart khôi phục matcher.

Release native dùng Rust 1.93, Cargo.lock, WASM tắt:
`sha256:2cd1764e7b6f8489227171a6fd8d8ce122372cc5d9971c874cc4c534703276e2`.
Source test bổ sung access-log variation sau build không đổi code binary release.
`scripts/http-routing-smoke.py` kiểm tra daemon/CLI thật với hai backend Go:
H1/H2, HEAD/POST, header match trước filter, hot apply trên keepalive, invalid
regex validate/apply, matched upstream unavailable trả 503 và restart. Raw:
`test-results/http-routing-final/summary.json` và commands/H2 header dumps.

SNI passthrough smoke chạy lại trên cùng image xác minh TLS/certificate đúng
backend, strict/missing/unknown/default, old TLS session qua hot apply,
upstream disable/enable và restart. Raw:
`test-results/http-routing-sni-default-final/summary.json`.
Không có HTTP header tên default tham gia SNI matching.

```sh
sudo python3 scripts/http-routing-smoke.py --image prost:native \
  --output-dir test-results/http-routing-new
sudo python3 scripts/passthrough-routing-smoke.py --image prost:native \
  --output-dir test-results/sni-default-new
```

Đây là kiểm thử chức năng, không phải benchmark. Soak đang chạy ở
`test-results/soak-24h-edge` vẫn dùng image 89da… trước các phần mới; không
dùng nó để khẳng định image 2cd1… đã qua 24 giờ.
