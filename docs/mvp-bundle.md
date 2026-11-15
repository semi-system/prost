# Native HTTP và transport: bản mở rộng MVP

Các tính năng dưới đây tắt mặc định. Ví dụ tổng hợp:
[advanced-native.toml](../examples/advanced-native.toml),
[mtls.toml](../examples/mtls.toml),
[proxy-protocol.toml](../examples/proxy-protocol.toml).
Đây là các tính năng cho pilot/MVP; test qua không thay thế soak hoặc đánh giá production.

## Routing và access log

Listener nhiều hơn 32 route dùng chỉ mục hash prefix tích lũy; mỗi hit vẫn so
sánh chuỗi để tránh lỗi collision. Giữ nguyên priority, prefix dài và thứ tự khai
báo, host không phân biệt hoa/thường. Memory chỉ mục tỷ lệ số route, không dựng
một node cho mỗi byte prefix dài. Listener ít route giữ linear scan.

Service HTTP có hai nhánh biên dịch riêng cho logging bật/tắt. Tránh boxing thêm
toàn bộ handler ở mỗi request khi logging bật; wrapper lớn chỉ xuất hiện ở mức
connection. Snapshot/route vẫn truyền trực tiếp, không qua request extensions.

`runtime.access_log.format`: `json` giữ schema đầy đủ; `compact` ghi JSONL với:
`t` timestamp Unix ms, `g` generation, `p` protocol, `r` peer, `l` listener,
`m` method, `u` path, `s` status, `b` DATA/relay bytes, `d` duration µs, `o` outcome.
Không phải format nginx tùy ý. Chính sách `include_query`, truncate và thời điểm
hoàn tất không đổi; xem [access-log.md](access-log.md).

`include_paths`/`exclude_paths` là prefix của path gốc trước rewrite; exclude thắng.
Path filter chạy trước sampling/ticket. `statuses` là danh sách status được ghi,
để trống ghi mọi status; kiểm tra sau response, không tiết kiệm chi phí ticket.
Sampling thực hiện trước status filter. TCP không có HTTP status nên không ghi
khi danh sách statuses không rỗng. Cấu hình sink/format/filter thuộc Runtime,
cần restart; flag listener/route vẫn hot apply. Metric
`prost_access_log_records_filtered_total` đếm path/status bị loại.

## Compression streaming

`upstreams.NAME.http.compression` cho gzip và Brotli (`gzip`, `br`), theo q-value
Accept-Encoding, thứ tự algorithms phân xử hòa. Mức mặc định 3, min 1024 byte,
tối đa 16 encoder đồng thời. Không queue chờ encoder; khi hết budget dùng identity
nếu client chấp nhận. Client loại identity mà không có encoding phù hợp nhận 406;
hết budget trong trường hợp này nhận 503.

Chỉ xử lý response 200 có MIME prefix trong content_types; bỏ HEAD, response
đã có Content-Encoding, Content-Range, no-transform, gRPC và SSE. Không buffer
toàn body; output chunk 16 KiB, giữ trailers/backpressure/cancellation. Codec có
thể gộp chunk nhỏ; không dùng gzip generic cho stream tương tác kiểu SSE/gRPC.
Xóa Content-Length, Accept-Ranges và digest không còn đúng; strong ETag đổi thành
weak ETag. Thêm Vary: Accept-Encoding ngay cả khi chọn identity. Native headers
chạy trước bước compression cuối; không cấu hình filter tự gán framing trái với body.

## Cache dùng chung có giới hạn

Cache RAM ở từng upstream, không lưu xuống đĩa. Chỉ GET đã hết request body, status
200 hoàn chỉnh, có `public` và max-age hoặc có s-maxage. Không heuristic TTL,
revalidation hoặc stale serving. Dùng Age/Date và giới hạn max_ttl_seconds;
thời gian đọc body cũng tính vào freshness. Entry hết hạn bị bỏ khi lookup.

Bỏ qua request có Authorization, Cookie, Range, conditional headers, Pragma,
hoặc cache directive cần tính toán thêm. Bỏ response private/no-store/no-cache,
Set-Cookie, bất kỳ Vary nào, Content-Range, gRPC hoặc có trailers. Key SHA-256
gồm URI, Host và Accept-Encoding; không dùng query/IP làm nhãn metrics.
`only-if-cached` trả 504 khi không có entry đáp ứng, không gửi request đó ra origin.

Payload/header cost giới hạn bởi max_bytes, max_entries và max_object_bytes;
tối đa min(128,max_entries) capture đồng thời. Buffer capture, entry đã eviction
nhưng còn DATA được client/driver giữ đều giữ reservation. Cạn budget thì bỏ capture,
traffic tiếp tục streaming. Bytes::from_owner giữ lease qua clone/slice của DATA.
max_bytes là budget logical payload/header cost, không phải trần RSS toàn daemon;
allocator/codec/network buffers và metadata có chi phí riêng. Eviction FIFO.

Cache lưu representation từ backend trước native response filters/compression;
hit vẫn chạy filters mỗi lần. Cấu hình cache/TLS/endpoint thay đổi tạo resource
mới; cập nhật weight/enable tương thích tái sử dụng cache/pool. Không persist cache
qua restart. Cache hit vẫn chịu admission/selection của request; nó không được
đếm là response mới từ backend trong metrics/passive health.

## Retry có kiểm soát

`http.retry` chỉ áp dụng GET/HEAD đã hết body và không upgrade. Không retry
POST/PUT, request streaming, TCP, WebSocket hoặc body lỗi sau response headers.
max_attempts gồm lần đầu (1–8), timeout_ms là budget tổng cho giai đoạn nhận headers
của tất cả lần; statuses chỉ nhận mã 5xx. Retry network/headers error trừ client body
fault. Retry **cùng endpoint đã chọn**, không phải proxy_next_upstream failover.
Endpoint balancing/health vẫn áp dụng cho request tiếp theo. Request không cấu
hình retry giữ semantics không retry cũ. Không buffer/replay request body.

Quản trị cache bằng Runtime API, purge có epoch bảo vệ fill đang chạy:
[cache-control.md](cache-control.md).

## Timeout H2

Chỉ khai báo dưới `listeners.http2_settings`; mặc định 0/tắt:

```toml
[listeners.http2_settings]
header_read_timeout_ms = 5000
stream_write_timeout_ms = 5000
```

Header guard quan sát framing sau TLS và preface, không tự decode HPACK. Giới hạn
partial frame header và toàn HEADERS/CONTINUATION block, kể cả sau request đầu.
Block chưa END_HEADERS không được interleave stream khác theo HTTP/2; khi quá
deadline đóng **connection**, không chèn RST giả vào state machine Hyper.
Idle connection chưa bắt đầu frame không bị tính là header timeout.

Write guard dùng executor cho từng H2 stream và owner của DATA trong queue h2.
Chia DATA tối đa 16 KiB; mỗi chunk có budget hoàn tất tính từ lúc enqueue đến khi
h2 không còn giữ owner. Đây là deadline theo chunk, không phải idle timer tự reset
theo mỗi byte: client rất chậm vẫn có thể bị ngắt dù có cập nhật window nhỏ.
Khi quá deadline (gồm initial window = 0 hoặc phần cuối frame bị kẹt), drop future
của stream để h2 reset nó; các stream khác tiếp tục. EOS/trailers được giữ đến khi
các owner đã drain, tránh future kết thúc trước DATA cuối. Khi upstream Pending
và không có DATA đang queue thì không có write timer; response_body_timeout vẫn quản lý upstream riêng.
Response 0 byte không cần write guard. Socket write timeout vẫn bảo vệ việc
socket/TLS bị chặn trên cả connection. Metric: prost_h2_stream_write_timeouts_total.
Không fork Hyper/h2; phần này dùng public Executor/body interfaces.

## TLS và PROXY protocol

`listeners.tls.client_ca` bật xác minh client certificate bắt buộc; không có cert
hợp lệ thì handshake thất bại. `upstreams.NAME.tls.identity` chứa certificate và
private_key để Prost xác thực với backend. Xác minh CA/hostname backend vẫn bật.
CA, certificate và key mới đều được stage vào artifact store, tham chiếu trong GC
và khôi phục sau restart. Rotation dùng apply; phiên cũ giữ snapshot cũ.

`tls_passthrough=true` trên TCP chuyển nguyên byte TLS tới endpoint, không terminate
TLS. Chọn backend theo SNI đã có qua `tls_passthrough_settings.routes`:
[passthrough-routing.md](passthrough-routing.md). Listener không có tls và upstream không được bọc
thêm TLS; plugin chọn upstream sai bị từ chối. Không phải STARTTLS.

`listeners.proxy_protocol` yêu cầu **một header ingress bắt buộc**, v1 hoặc v2,
trước TLS. trusted_peers kiểm tra IP socket thật; không tin địa chỉ do client tự
khai báo khi nguồn chưa được cho phép. V1 tối đa 108 byte, v2 payload tối đa 512,
timeout mặc định 2 giây. TCP4/TCP6 và LOCAL/UNKNOWN được hỗ trợ; không dùng TLV
để thay chứng thực TLS. Địa chỉ client được dùng cho ACL/rate/X-Forwarded-For/log;
quyết định tin forwarded headers dùng peer transport thật. PROXY v2 egress, pool HTTP giữ đúng IP, chính sách TLS,
health check mail và giới hạn IP đã có trong [rusty.md](rusty.md).

## Metrics tùy chọn

`upstreams.NAME.observe_pool_state=true` dùng public capture_connection của
hyper-util để biết transport được gán ngay trước send/headers. Lease giữ đến body,
trailers, cancel hoặc tunnel end. Một socket H2 có nhiều stream vẫn chỉ busy một
lần. `connections_busy`/`connections_idle` là transport mở có/không có assignment,
không phải trạng thái private idle queue của Hyper. Scrape không nguyên tử; busy
được clamp theo số transport có tracking. Transport cũ tạo khi tracking tắt
được xuất riêng ở connections_untracked, không gộp nhầm vào idle sau hot toggle.
Basic open/close vẫn có khi tracking tắt; reuse độc lập.

`runtime.observe_plugins=true` bật counter/duration theo **plugin type và hook**,
gộp listener/route cùng tên, không sinh series theo URL/config hash. Native filter,
native event plugin, connection_limit và WASM đều được đo. Counter giữ qua apply;
type không còn trong config không được export. Metric:
prost_plugin_calls_total{plugin,hook,result} và prost_plugin_seconds_total.
Result success/error/rejected/cancelled; WASM duration gồm thời gian chờ execution.

Cache: prost_http_cache_hits_total/misses_total/evictions_total/bytes/entries.
Retry: prost_http_retry_attempts_total đếm lần bổ sung, không gồm lần đầu.

## Soak local

Hướng dẫn image Baseless đã publish, chạy nền trên VM và các prerequisite:
[vm-soak.md](vm-soak.md). Build backend fixture và chuẩn bị certificate một lần:

```sh
sudo docker build -t prost:bundle-backend benchmarks/advanced
docker pull python:3.13-alpine
python3 scripts/prepare-advanced-bench.py
sudo python3 scripts/soak.py --image prost:native --duration 60 --interval 5 \
  --output-dir test-results/soak-smoke
sudo python3 scripts/soak.py --image prost:native --duration 86400 \
  --output-dir test-results/soak-24h
```

H1 mixed workload có kiểm tra byte, H2/TLS probes, TCP session cũ qua rolling reload,
rotate log, artifact GC, RSS/FD/task/generation samples. Log giữ tối đa file hiện
tại và một file rotated. Threshold là guard cho fixture, không chứng minh capacity
production: RSS <192 MiB, FD/task <512; mỗi reload đóng phiên TCP đã kiểm tra qua
commit rồi chờ drain, yêu cầu 1 generation còn sống và 0 forced drain.
H1 dùng connection mới, không retry lỗi; helper riêng rotate log cho image không
có shell. Gate tài nguyên lấy mẫu sau drain, không đo mọi đỉnh tức thời.
Cuối bài yêu cầu 0 connection, 1 generation và 0 traffic error.

progress.json ghi starting/running/passed/failed; chỉ có summary.json sau khi đủ
thời gian và kiểm tra cuối thành công. samples.jsonl và final-metrics.prom giữ
bằng chứng. Một lượt 60 giây không được gọi là soak 24 giờ. Không chạy benchmark
throughput đồng thời với soak. Kết quả bản build hiện tại sẽ ghi trong
[mvp-bundle-results.md](mvp-bundle-results.md).

Tham khảo API/ngữ nghĩa: [HTTP caching RFC 9111](https://www.rfc-editor.org/rfc/rfc9111.html),
[Hyper H2 builder](https://docs.rs/hyper/1.12.0/hyper/server/conn/http2/struct.Builder.html),
[h2 builder](https://docs.rs/h2/0.4.20/h2/server/struct.Builder.html).
