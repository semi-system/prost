# Native header engine

Engine tích hợp vào binary Rust và thao tác trực tiếp trên `HeaderMap`.
Config TOML được biên dịch khi validate/apply: tên header, literal, biến, rule
status và expires tĩnh được chuẩn bị trước commit. Config mới có hiệu lực trên
request mới, kể cả keep-alive/HTTP2; response đang stream giữ generation cũ.
Thay header không reset rate-limit/connection-limit bên cạnh nếu config và vị
trí các plugin đó không đổi.

## Các phép xử lý

| Chức năng | Config Prost |
|---|---|
| Gán header upstream request | `request.set` |
| Xóa header request | `request.remove` |
| Thay header response | `response.set` |
| Thêm nhiều giá trị, gồm Set-Cookie | `request.append`, `response.append` |
| Ẩn header response | `response.remove` |
| Thêm header theo status, tùy chọn always | `response.add` |
| Thêm trailer theo status, tùy chọn always | `trailers` |
| Expires và Cache-Control | `expires` |
| Kế thừa header giữa listener/route | `inherit` |
| Kế thừa trailer giữa listener/route | `trailer_inherit` |
| Map từ biến sang giá trị, tính khi dùng | `maps` |
| Rule có điều kiện, giữ thứ tự khai báo | `request.rules`, `response.rules`, `when` |

Đây là schema của Prost; không đọc trực tiếp cú pháp cấu hình nginx và không
triển khai toàn bộ biến/modules của nginx. Phạm vi tương ứng nhóm chức năng
header, trailer, expires và request/response header rewriting.

```toml
[[listeners.plugins]]
name = "headers"
config = { request = { set = { x-request-id = "$request_id" }, remove = ["x-debug"] }, response = { remove = ["server", "x-powered-by"], add = [{name = "x-proxy", value = "prost", always = true}, {name = "x-request-id", value = "$request_id", always = true}, {name = "x-upstream", value = "$upstream_addr"}] }, expires = "off", trailers = [{name = "x-end-status", value = "$status", always = true}] }
```

Ví dụ đầy đủ dùng được với Compose demo:
[examples/native-headers.toml](../examples/native-headers.toml).

```sh
docker compose exec prost prostctl validate --file /examples/native-headers.toml
docker compose exec prost prostctl apply --file /examples/native-headers.toml
curl --http1.1 -H 'TE: trailers' -i http://127.0.0.1:8080/
```

Demo mặc định chỉ thêm header literal `x-proxy: prost`, không tự bật request ID,
expires hoặc trailer. Cấu hình mẫu đầy đủ bật những tính năng đó để kiểm tra.

## Thứ tự, status và always

Mỗi phase xử lý `remove → set → append → add → rules`. `set` thay toàn bộ giá trị cũ;
`append`/`add` thêm từng giá trị và giữ field cũ. `set`/`append` áp dụng trên mọi
status. `response.add` mặc định áp dụng 200, 201, 204, 206, 301, 302, 303, 304,
307, 308; `always = true` áp dụng cả status khác và lỗi do proxy sinh ra.
Giá trị `add` render thành rỗng được bỏ qua.

Native filters xử lý cả response cục bộ 403, 404, 429, 503. WASM/event hooks
không được gọi lại để xử lý lỗi do chính các hook sinh ra.

```toml
[[listeners.plugins]]
name = "headers"
config = { response = { add = [{name = "cache-control", value = "private"}, {name = "x-status", value = "$status", always = true}], append = { set-cookie = ["a=1; Path=/", "b=2; Path=/"] } } }
```

## Biến

Hỗ trợ `$name`, `${name}`, `$$` để ghi literal `$`. Biến không biết bị từ chối
khi apply, không parse tên biến trên đường xử lý traffic.

| Biến | Giá trị |
|---|---|
| `$remote_addr` | IP TCP peer, không lấy từ forwarded header |
| `$scheme` | `http` hoặc `https` theo listener TLS |
| `$host` | Host lowercase, bỏ port |
| `$request_method` | Method request |
| `$request_uri` | Path và query nguyên bản |
| `$uri` | Path hiện tại sau rewrite, không query; không giải mã percent-encoding |
| `$request_id` | 16 byte random, biểu diễn 32 ký tự hex, dùng chung trong một request |
| `$status` | Status response |
| `$upstream_addr` | Địa chỉ kết nối upstream; fallback địa chỉ endpoint nếu connector không cung cấp HttpInfo |
| `$http_<name>` | Header request gốc; underscore trong tên biến đổi thành hyphen |
| `$upstream_http_<name>` | Header response upstream trước khi filter sửa |
| `$sent_http_<name>` | Header hiện tại tại thời điểm render rule |

`upstream_*` và `sent_http_*` dành cho response/trailer phase. Header thiếu cho
giá trị rỗng; nhiều giá trị được nối bằng dấu phẩy, riêng Cookie dùng dấu chấm
phẩy. Các rule response chạy theo thứ tự khai báo nên `$sent_http_*` nhìn thấy
thay đổi của rule trước nó. Trailer render sau các header filters.

Chỉ tạo request ID nếu có rule dùng biến đó. Chỉ giữ những header input mà
biểu thức đã đăng ký; không clone toàn bộ header map. Biến đơn có sẵn dưới dạng
`HeaderValue` được tái sử dụng; literal không dựng chuỗi trên mỗi request.

## Map và điều kiện

Map được khai báo ngay trong config của plugin `headers`, rồi dùng như biến
`$allowed_origin`. Map có thể tham chiếu map khác; tên trùng biến builtin, biến
không tồn tại và chu trình tham chiếu đều bị từ chối trước commit.

```toml
[[listeners.plugins]]
name = "headers"
[listeners.plugins.config]
maps = { allowed_origin = { source = "$http_origin", default = "", entries = [{ pattern = "https://app.example", value = "$http_origin" }, { kind = "regex", pattern = '^https://([a-z]+)\.example$', value = "https://$1.example" }] } }
response = { add = [{ name = "access-control-allow-origin", value = "$allowed_origin", always = true, when = { source = "$allowed_origin", not_empty = true } }], rules = [{ action = "remove", name = "x-debug", when = { source = "$status", matches = "^[45]" } }] }
```

Mỗi map có `source`, `default` (mặc định rỗng), `case_sensitive` (mặc định false),
`volatile` (mặc định false), và danh sách `entries`. Entry có `pattern`, `value`,
`kind` thuộc `exact` (mặc định), `prefix`, `suffix`, `regex`, `regex_i`.
Thứ tự ưu tiên: exact → prefix dài nhất → suffix dài nhất → regex theo thứ tự
khai báo → default. Prefix/suffix cùng độ dài giữ thứ tự khai báo.
Exact/prefix/suffix mặc định không phân biệt hoa thường ASCII; regex phân biệt
hoa thường, `regex_i` thì không. Trong value của entry regex có thể dùng `$1`–`$9`.
Regex dùng Rust regex, không hỗ trợ lookaround/backreference của PCRE.

Map chỉ được tính khi rule tham chiếu đến nó; map không dùng không tạo metadata
request. Kết quả được cache **trong từng phase của từng plugin**, không dùng chung
giữa request/response/trailer hoặc plugin khác. `volatile = true` tính lại mỗi
lần dùng, hữu ích với `$sent_http_*` sau khi rule trước sửa header. Bản thân map
volatile tham chiếu map khác vẫn tuân theo chính sách cache của map đó.
Đây là phạm vi của Prost; không cung cấp cú pháp wildcard `hostnames` của nginx.

`when` áp dụng cho `add`, trailer và `rules`. Mỗi điều kiện gồm `source` và đúng
một phép so sánh: `equals`, `not_equals`, `matches`, `not_matches`, `not_empty`
hoặc `truthy`. Hai vế equals là template; matches là regex tĩnh.
`not_empty = true` kiểm tra khác rỗng; `truthy = true` kiểm tra khác rỗng và khác
`"0"`; đặt false đảo điều kiện. So sánh equals phân biệt hoa thường.
`rules` chạy tuần tự với `action = "set" | "append" | "remove"`; set/append cần
`value`, remove không nhận value. Rule không có when luôn chạy. `add` vẫn tuân
theo status/always trước khi xét when; rule set/append/remove không lọc status.

Ví dụ đầy đủ gồm map, expires động và rate limit theo API key:
[examples/native-policies.toml](../examples/native-policies.toml). Demo này giới
hạn 1 request/giây/key, burst 2, để dễ quan sát HTTP 429; đổi rate trước khi dùng
cho workload thực tế. Giá trị API key trong ví dụ chỉ dùng làm key giới hạn,
không thực hiện xác thực.

Mỗi plugin tối đa 16 map, tổng 256 entry và 8 regex (gồm regex trong when).
Pattern tối đa 1 KiB; regex được compile trước commit với giới hạn kích thước
máy trạng thái. Cache giá trị map tối đa 32 KiB/phase; tối đa 128 lần tính map
chưa cache/phase, kể cả volatile, để chặn đồ thị tham chiếu có chi phí tăng quá
nhanh. Vượt budget là lỗi filter (503). Đường xử lý không dùng map
không cấp phát cache map.

## Expires

`expires` áp dụng trên cùng danh sách status mặc định của `response.add`, chạy
trước response rules; rule sau có thể sửa Cache-Control/Expires nếu cần.

| Giá trị | Hành vi |
|---|---|
| `off` | Không sửa Expires/Cache-Control; có thể tắt expires kế thừa |
| `0`, `1h30m`, `7d` | Hạn theo thời gian hiện tại, max-age tương ứng |
| `-1s` | Hạn trong quá khứ, Cache-Control no-cache |
| `modified +1h` | Dựa vào Last-Modified, thiếu hoặc sai thì dùng hiện tại |
| `epoch` | Hạn ngày 1970-01-01 00:00:01 GMT, no-cache |
| `max` | Hạn 2037-12-31 23:55:55 GMT, max-age 10 năm |
| `@15h30m` | Mốc ngày tiếp theo theo UTC |
| `$http_x_ttl` | Render biến rồi kiểm tra interval; giá trị động sai bỏ qua expires |

Đơn vị: s, m, h, d, w, M (30 ngày), y (365 ngày); interval tối đa 100 năm.
Mốc `@` tối đa 24 giờ, dùng UTC và không kết hợp `modified`.
Expires tĩnh được parse khi apply. Expires động chỉ parse khi tính năng được dùng.

## Kế thừa

Mặc định `inherit = "on"`: route có rule request riêng thì thay nhóm request
header kế thừa; có rule response riêng thì thay nhóm response kế thừa. Route
không có rule cho phase đó vẫn dùng nhóm từ listener.
`off` tắt cả hai nhóm header kế thừa; `merge` chạy listener rồi route.
Expires kế thừa riêng: route không khai báo vẫn dùng listener, khai báo `off`
để tắt. `trailer_inherit` độc lập, cũng có `on`/`off`/`merge`.

Nếu khai báo nhiều plugin `headers` trên cùng route, plugin đầu quyết định
chính sách kế thừa; các plugin trong route vẫn chạy theo thứ tự khai báo.

## Trailer và streaming

```toml
[[listeners.plugins]]
name = "headers"
config = { trailers = [{name = "x-completed", value = "$request_id:$status", always = true}] }
```

HTTP/1.1 client phải gửi `TE: trailers`; body chuyển sang chunked và tên trailer
được công bố. HTTP/2 thêm frame trailer, giữ trailer upstream như `grpc-status`.
Không thêm trailer cho HTTP/1.0, HEAD, 1xx, 204, 304. Không gom body vào RAM;
wrapper chỉ giữ trailer đã render. Admission/generation/completion guard được
giữ tới trailer hoặc cancellation.

## Giới hạn

Mỗi phía request/response tối đa 64 thao tác, 32 KiB rule tĩnh. Trailer có giới
hạn riêng tương tự. Mỗi giá trị template tối đa 8 KiB sau render, output mỗi
filter/phase tối đa 32 KiB. Không cho rule tự sửa framing như Content-Length,
Transfer-Encoding, Connection hoặc những trường bị cấm trong trailer.
Lỗi cú pháp, biến lạ và literal sai bị từ chối trước commit. Giá trị động không
hợp lệ là lỗi filter khi chạy; request chưa gửi response nhận 503.

Tham khảo đối chiếu nhóm chức năng:
[nginx headers module](https://nginx.org/en/docs/http/ngx_http_headers_module.html).
