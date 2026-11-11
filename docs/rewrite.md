# Native rewrite và redirect

Plugin `rewrite` chạy trong request phase trên I/O worker, không tạo event/JSON
hoặc blocking task. Rule được compile khi validate/apply và cập nhật qua Runtime
API. Cấu hình đầy đủ dùng với demo: [examples/rewrite.toml](../examples/rewrite.toml).

```toml
[[listeners.plugins]]
name = "rewrite"
config = { rules = [{ action = "redirect", exact = "/legacy", to = "/new", status = 308 }, { action = "rewrite", prefix = "/api/v1/", to = "/api/v2/" }, { action = "rewrite", regex = '^/users/([0-9]+)$', to = "/profiles/$1?source=legacy", query = "append", when = { source = "$request_method", equals = "GET" } }] }
```

## Match và thứ tự

Mỗi rule chọn tối đa một matcher `exact`, `prefix` hoặc `regex`. Không có matcher
nghĩa là mọi path, có thể thêm `when` để giới hạn. Match chỉ xét path, không xét
query, không phân biệt theo method trừ khi có when; path phân biệt hoa thường.

- `exact`: thay toàn bộ path.
- `prefix`: thay prefix, giữ nguyên phần path còn lại. `/api/v1/item` với
  `prefix = "/api/v1/", to = "/api/v2/"` thành `/api/v2/item`.
- `regex`: match path hiện tại rồi thay toàn bộ path bằng `to`, hỗ trợ `$1`–`$9`.
  Dùng Rust regex; không có PCRE lookaround/backreference. Khi không dùng capture
  trong output, plugin chỉ kiểm tra match, không dựng danh sách capture.

Rule chạy theo thứ tự khai báo. Rewrite mặc định `stop = true`, dừng các rule còn
lại của plugin đó; plugin tiếp theo vẫn chạy. `stop = false` cho rule sau xử lý
URI vừa đổi. Redirect luôn kết thúc request phase ngay khi khớp.

Listener và route chạy plugin theo thứ tự hiện tại: listener trước, route sau.
Đặt ACL/rate_limit **trước** rewrite nếu cần kiểm tra chúng trước redirect. Plugin
sau redirect không chạy request hook. Native response header/trailer filters vẫn
áp dụng cho redirect theo policy inheritance; event/WASM response/end hooks chưa
được gọi cho response cục bộ, giống các response lỗi do proxy sinh ra.

Route/upstream được chọn theo URI gốc và giữ nguyên sau rewrite. Không có bước
reroute kiểu nginx `last` hoặc vòng lặp internal redirect. Request không có
route/upstream vẫn chạy request filters để cho phép listener chỉ redirect; nếu
không filter nào trả response, proxy trả 404.

## Target và query

Rewrite chỉ nhận origin path bắt đầu bằng `/`, không có authority hoặc fragment;
không đổi method, headers hay body. Scheme/authority upstream vẫn do cấu hình
upstream quyết định. Redirect nhận path `/new` hoặc URL http(s) đầy đủ, hỗ trợ
fragment và các status 301, 302 (mặc định), 303, 307, 308. Response redirect có body
rỗng. Method của request tiếp theo phụ thuộc status và hành vi client.

| `query` | Target không có `?` | Target đã có `?` |
|---|---|---|
| `preserve` (mặc định) | Giữ query hiện tại | Dùng query trong target |
| `drop` | Bỏ query hiện tại | Dùng query trong target |
| `append` | Giữ query hiện tại | Nối query hiện tại sau query trong target bằng `&` |

Target `/new?` với preserve chủ động bỏ args cũ. Query được xử lý trước fragment.
Prefix nối suffix vào path của URL redirect, không nối vào hostname. Không tự
decode hoặc encode percent-escape; `/a%2Fb` vẫn giữ nguyên `%2F`.
Không chấp nhận target rỗng, control/whitespace, backslash, protocol-relative
`//host`, hoặc URL redirect có userinfo. Target động không hợp lệ trả 503 trước
khi mở kết nối upstream.

## Biến và điều kiện

Hỗ trợ các biến request trong [native-headers.md](native-headers.md): `$scheme`,
`$host`, `$request_method`, `$remote_addr`, `$request_id`, `$http_<name>`, cùng:

- `$request_uri`: path + query nguyên bản, giữ nguyên qua chuỗi rewrite.
- `$uri`: path hiện tại tại thời điểm render; header filter sau rewrite, response
  header và trailer cũng nhìn thấy path đã rewrite.
- `$1`–`$9`: capture của matcher regex trong chính rule đó, chỉ dùng trong `to`.

Biến response/upstream không được dùng trong rewrite. Map của plugin headers có
phạm vi riêng và không được chia sẻ sang plugin rewrite. `$http_*` lấy request
headers gốc đã đăng ký; không copy toàn bộ HeaderMap.

`when` có cùng cú pháp equals/not_equals/matches/not_matches/not_empty/truthy như
native header conditions. Source `$uri` nhìn thấy path hiện tại; regex condition
không ghi đè capture của matcher. Ví dụ redirect HTTP sang HTTPS:

```toml
[[listeners.plugins]]
name = "rewrite"
config = { rules = [{ action = "redirect", to = "https://example.com$request_uri", status = 308, query = "drop", when = { source = "$scheme", equals = "http" } }] }
```

## Streaming, cập nhật và tài nguyên

Không đọc/gom body để rewrite. HTTP/1.1, HTTP/2 và gRPC giữ streaming, backpressure,
method và trailers. Request/stream mới lấy config mới trên cùng kết nối; request
đang gửi body hoặc nhận response giữ generation cũ. Redirect cũng dùng request
admission hiện tại; không kết nối upstream và không có queue riêng.

Mỗi plugin 1–32 rule, tối đa 32 KiB config rule, 8 regex gồm when, pattern 1 KiB,
output 8 KiB và capture 32 KiB. Regex/condition/template/literal URI được chuẩn bị
trước commit. Cấu hình sai giữ config/generation cũ. Mỗi rule xét tối đa một lần
trong mỗi lần gọi plugin, không lặp đến khi URI ổn định.

Route không dùng rewrite không clone thêm URI. Target literal được parse sẵn;
redirect literal và rewrite literal không cần ghép suffix/query dùng lại giá trị
đã chuẩn bị. URI cần ghép động có chuỗi tạm giới hạn 8 KiB.
Phần xử lý URI được tách ra future riêng, chỉ cấp phát khi route bật filter cần
URI; metadata URI đã đổi cũng chỉ cấp phát khi request cần context. Redirect
literal dùng lại response head qua Arc, không làm enum quyết định của các filter
header/ACL/rate-limit phải mang HeaderValue trên đường chạy mặc định.

```sh
docker compose exec prost prostctl validate --file /examples/rewrite.toml
docker compose exec prost prostctl apply --file /examples/rewrite.toml
curl -i 'http://127.0.0.1:8080/legacy?keep=1'
curl 'http://127.0.0.1:8080/api/v1/item?q=1'
curl 'http://127.0.0.1:8080/users/42?q=1'
```

Phạm vi này không cung cấp toàn bộ directive nginx rewrite/return/error_page.
Đối chiếu: [nginx rewrite module](https://nginx.org/en/docs/http/ngx_http_rewrite_module.html).

Số đo nginx cùng workload và đối chứng image trước/sau:
[rewrite-benchmark.md](rewrite-benchmark.md).
