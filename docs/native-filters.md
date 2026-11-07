# Native filters tích hợp sẵn

Các filter được build vào binary Rust. Thêm code mới cần build lại; thay rule
inline qua Runtime API không cần restart. WASM vẫn dùng cho phần mở rộng động.

| Filter | Chức năng | Tài nguyên khi xử lý request |
|---|---|---|
| `headers` | Set, append nhiều giá trị, remove header request/response | Tên/giá trị parse trước commit; thao tác trực tiếp `HeaderMap` |
| `ip_acl` | Allow/deny CIDR IPv4/IPv6 | Danh sách parse sẵn, tối đa 256 CIDR; không có bảng theo IP |
| `rate_limit` | Giới hạn request với burst; HTTP 429 + Retry-After | Một bucket atomic chung các worker cho mỗi plugin; không queue |
| `connection_limit` | Giới hạn phiên trên listener | Semaphore permit giữ tới khi phiên/tunnel kết thúc |

Chỉ dùng các filter native này thì request không tạo `Event`, không chuyển
header thành `BTreeMap`/JSON, không tạo WASM instance và không gọi
`spawn_blocking`. HeaderMap vẫn có thể cấp phát khi thêm field mới; không tuyên
bố toàn bộ HTTP pipeline không cấp phát. Engine WASM chỉ khởi tạo khi config có
module WASM, nên cấu hình thuần native không chịu chi phí engine.

## Header request và response

```toml
[[listeners.plugins]]
name = "headers"
config = { request = { set = { x-proxy = "prost" }, remove = ["x-debug"] }, response = { set = { x-proxy = "prost" }, append = { vary = ["Origin"] }, remove = ["x-powered-by"] } }
```

Trong mỗi phase: remove → set → append. `set` thay toàn bộ giá trị cũ của tên đó;
`append` giữ giá trị cũ và thêm từng field, phù hợp với header nhiều giá trị như
`Set-Cookie`. Thứ tự các plugin vẫn theo khai báo, listener trước route. Header
native trước WASM sẽ được WASM nhìn thấy và ngược lại.

Không đổi body hoặc framing; các tên `content-length`, `transfer-encoding`,
`connection`, `keep-alive`, `proxy-connection`, `upgrade`, `te`, `trailer` bị từ
chối trong filter này. Header name/value sai hoặc rule quá giới hạn bị từ chối
trước commit. Mỗi phía tối đa 64 thao tác, tổng name/value set/append 32 KiB.
Giá trị là literal; chưa có biến nginx như `$request_id` hay template.

Response rule chạy cho response đã lấy từ upstream, kể cả upstream trả 4xx/5xx.
Response 403/429/503 do chính proxy tạo hiện chưa chạy lại response filters.
Đây là khác biệt với nginx `add_header ... always` trên response cục bộ.

## ACL

```toml
[[listeners.plugins]]
name = "ip_acl"
config = { allow = ["10.0.0.0/8", "2001:db8::/32"], deny = ["10.1.2.0/24"] }
```

Deny được ưu tiên; allow rỗng nghĩa là cho phép những địa chỉ không thuộc deny.
Allow có phần tử nghĩa là địa chỉ còn phải thuộc một CIDR allow. Dùng địa chỉ
TCP peer thật, không lấy từ `X-Forwarded-For`. HTTP bị từ chối trả 403; TCP đóng
phiên trước khi kết nối backend. ACL trên route chỉ áp dụng request route đó.
Đây là quy tắc deny-trước, khác với thứ tự first-match của nginx access module.

## Rate limit

```toml
[[listeners.plugins]]
name = "rate_limit"
config = { requests_per_second = 10000, burst = 200 }
```

Bucket GCRA chung cho các worker và connection trong plugin đó. Burst 200 cho
phép tối đa 200 request ngay khi bucket trống, sau đó refill theo rate. Rate và
burst trong khoảng 1–1.000.000. Request vượt budget nhận 429 ngay, không xếp
hàng, có `Retry-After` theo giây. Không giới hạn theo IP/key, không có map tăng
theo số client. Đặt trên route để giới hạn riêng route; đặt trên listener để
chia budget cho mọi route của listener.

Nếu plugin config không đổi, apply upstream/pool tái sử dụng bucket và permit.
Thay plugin config tạo bucket mới. Trạng thái tiêu thụ budget không lưu qua
restart; các rule được lưu trong managed config như những plugin khác.

## Chỉ chạy hook cần thiết

Native và WASM đều hỗ trợ bộ lọc hook tùy chọn:

```toml
[[listeners.plugins]]
name = "wasm"
module = "/plugins/headers.wasm"
hooks = ["response_headers"]
config = {}
```

Không khai báo `hooks` giữ hành vi cũ cho plugin event/WASM. Mảng rỗng tắt các
hook, module vẫn được validate. Tên hợp lệ: `connection`, `request_headers`,
`upstream`, `response_headers`, `end`. Tên lạ hoặc lặp bị từ chối.
`connection_limit` không nhận bộ lọc hook. Native builtin tự khai báo phase phù
hợp; không cần tự ghi `hooks` cho chúng. Khi không plugin nào đăng ký `end`,
request không tạo completion channel/end-hook task.

## Thêm native filter bằng Rust

`prost_plugin::NativeFilter` nhận `FilterHook`, `IpAddr` và trực tiếp
`Option<&mut http::HeaderMap>`; trả `FilterDecision::Continue` hoặc
`FilterDecision::Reject { status, retry_after }`. Đăng ký bằng
`PluginRegistry::register_filter(name, Arc<dyn NativeFilter>)`, rồi truyền
registry cho `ProxyBuilder::plugins` trước khi start.

Callback chạy trên I/O worker, phải trả nhanh và không block. `Connection` của
filter trực tiếp áp dụng cho TCP; HTTP dùng `RequestHeaders`/`ResponseHeaders`.
API `NativePlugin::on_event` cũ vẫn dùng được cho các hook cần chọn upstream hoặc
logic event; API đó vẫn có bước tạo event/header map. Cả hai được build cùng
binary, không nạp `.so` động.

Tham khảo chức năng nginx:
[headers](https://nginx.org/en/docs/http/ngx_http_headers_module.html),
[access](https://nginx.org/en/docs/http/ngx_http_access_module.html),
[limit_req](https://nginx.org/en/docs/http/ngx_http_limit_req_module.html).
