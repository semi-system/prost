# Native filters tích hợp sẵn

Các filter được build vào binary Rust. Thêm code mới cần build lại; thay rule
inline qua Runtime API không cần restart. Build mặc định thuần native; WASM là
feature tùy chọn `prost-cli/wasm`.

| Filter | Chức năng | Tài nguyên khi xử lý request |
|---|---|---|
| `headers` | Set, append nhiều giá trị, remove header request/response | Tên/giá trị parse trước commit; thao tác trực tiếp `HeaderMap` |
| `ip_acl` | Allow/deny CIDR IPv4/IPv6 | Danh sách parse sẵn, tối đa 256 CIDR; không có bảng theo IP |
| `rate_limit` | Giới hạn toàn cục, theo IP hoặc header; HTTP 429 + Retry-After | GCRA chung các worker; bảng key có giới hạn, không queue |
| `connection_limit` | Giới hạn phiên trên listener | Semaphore permit giữ tới khi phiên/tunnel kết thúc |

Chỉ dùng các filter native này thì request không tạo `Event`, không chuyển
header thành `BTreeMap`/JSON, không tạo WASM instance và không gọi
`spawn_blocking`. HeaderMap vẫn có thể cấp phát khi thêm field mới; không tuyên
bố toàn bộ HTTP pipeline không cấp phát. Build mặc định không link Wasmtime.
Khi bật feature WASM, engine chỉ khởi tạo nếu config có module WASM.

## Header request và response

```toml
[[listeners.plugins]]
name = "headers"
config = { request = { set = { x-proxy = "prost" }, remove = ["x-debug"] }, response = { set = { x-proxy = "prost" }, append = { vary = ["Origin"] }, remove = ["x-powered-by"] } }
```

Trong mỗi phase: remove → set → append → add → rules. `set` thay toàn bộ giá trị cũ của tên đó;
`append` giữ giá trị cũ và thêm từng field, phù hợp với header nhiều giá trị như
`Set-Cookie`. Thứ tự các plugin vẫn theo khai báo. Listener/route hỗ trợ
`inherit` và `trailer_inherit` với `on`, `off`, `merge`.

Không đổi body hoặc framing; các tên `content-length`, `transfer-encoding`,
`connection`, `keep-alive`, `proxy-connection`, `upgrade`, `te`, `trailer` bị từ
chối trong filter này. Header name/value sai hoặc rule quá giới hạn bị từ chối
trước commit. Mỗi phía tối đa 64 thao tác và 32 KiB rule tĩnh; giá trị động tối
đa 8 KiB mỗi giá trị, 32 KiB output mỗi filter/phase.

`set`/`append` chạy trên mọi status. `response.add` chọn status mặc định hoặc
`always`, gồm lỗi cục bộ 403/404/429/503. Có biến request/upstream, request ID,
expires/cache-control và trailer HTTP/1.1–HTTP/2.
Ví dụ và hành vi chi tiết: [native-headers.md](native-headers.md).

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
hàng, có `Retry-After` theo giây. Mặc định `key = "global"` chỉ dùng atomic,
không tạo bảng theo client. Đặt trên route để giới hạn riêng route; đặt trên listener để
chia budget cho mọi route của listener.

Giới hạn riêng theo IP TCP peer hoặc header:

```toml
[[listeners.plugins]]
name = "rate_limit"
config = { requests_per_second = 100, burst = 20, key = "$remote_addr", max_keys = 4096, idle_timeout_ms = 60000 }
# Hoặc key = "$http_x_api_key". Header key tối đa 256 byte;
# header thiếu/rỗng dùng chung một bucket, không bypass giới hạn.
```

Key IP không tin `X-Forwarded-For`. Key header lấy giá trị đầu tiên tại lúc filter
chạy, chịu ảnh hưởng của plugin request trước nó; không nên dùng header do client
tự chọn để thay thế quota theo IP trong trường hợp cần chống đổi key.
Mỗi key có bucket chia sẻ giữa mọi worker/connection của plugin.
`max_keys` mặc định 4096, phạm vi 1–65536; idle timeout mặc định 60 giây,
phạm vi 100 ms–24 giờ. Bảng chia tối đa 16 shard với quota cố định để giới hạn RAM;
phân bố lệch có thể làm một shard đầy trước tổng max_keys. Key mới khi shard đầy
nhận 503 + Retry-After: 1; key đã có vẫn xử lý bình thường. Key header quá dài
nhận 400. Chỉ copy key khi thêm mới, không giữ nguyên buffer HTTP đầu vào.

Key idle được thu hồi khi có traffic tới shard đó (tối đa một lần/giây), chỉ sau
khi hết nợ rate và không còn tham chiếu đang xử lý. Không có task riêng cho từng
key; khi không có traffic, bảng giữ lượng bộ nhớ đã giới hạn cho tới traffic tiếp
theo hoặc khi plugin bị thu hồi. Giới hạn keyed không lưu trạng thái qua restart.

Nếu plugin giữ config và vị trí trong danh sách, apply upstream/pool hoặc sửa
header bên cạnh tái sử dụng bucket và permit. Chỉ plugin thay đổi được chuẩn bị
lại. Toàn bộ danh sách không đổi vẫn tái sử dụng cùng Arc.
Thay plugin config tạo bucket mới. Trạng thái tiêu thụ budget không lưu qua
restart; các rule được lưu trong managed config như những plugin khác.

## Chỉ chạy hook cần thiết

Native và WASM tùy chọn đều hỗ trợ bộ lọc hook. Muốn dùng ví dụ WASM dưới đây,
build bằng `cargo build --release --features prost-cli/wasm`, hoặc Docker build
với `--build-arg CARGO_FEATURES=prost-cli/wasm`:

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
