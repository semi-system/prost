# DNS refresh và cache revalidation

Kết quả kiểm chứng: [dns-cache-results.md](dns-cache-results.md).

## DNS backend

```toml
[upstreams.app.dns]
ttl_ms = 30000
negative_ttl_ms = 1000
timeout_ms = 2000
max_hosts = 1024
max_addresses = 32
```

Đây là TTL **do người vận hành cấu hình**, không phải TTL authoritative lấy
từ DNS server. Resolver hệ thống (`getaddrinfo`) chỉ cung cấp địa chỉ; Prost
cache kết quả theo policy này. Không khai báo block `dns` giữ cách dial cũ.

Request/dial hoặc health check đầu tiên sau expiry refresh hostname. Không có
thread polling file hay watcher cấu hình. Lookup đồng thời cùng hostname được
gộp; negative answer/timeout được giữ theo negative TTL. Literal IPv4/IPv6 bỏ
qua lookup. Nhiều IP được giới hạn, loại trùng và xoay thứ tự; TCP thử các địa
chỉ theo thứ tự trong deadline kết nối. Đây chưa phải Happy Eyeballs hay bộ
discovery SRV/HTTPS record.

Tối đa 32 công việc resolver hệ thống chạy đồng thời trong process. Permit
được giữ trong blocking task cả khi caller timeout, để lookup OS chậm không
tạo hàng đợi vô hạn. Không thể hủy `getaddrinfo` đang chạy. Mỗi upstream giới
hạn hostname/IP; apply từ chối `max_hosts` nhỏ hơn số hostname endpoint cần
dùng. Không trả IP cũ khi refresh lỗi.

TCP, TLS backend, HTTP và health check dùng chung resolver của upstream. Khi
tập IP thay đổi, HTTP request/HTTP2 stream mới đổi sang pool mới; URI, Host và
TLS server name vẫn dùng cấu hình ban đầu. Stream/phiên đang chạy giữ kết nối
cũ. HTTP PROXY v2 vẫn có pool riêng theo frontend và cũng theo DNS revision.
Đổi IP có thể làm mất các idle connection của endpoint khác cùng upstream;
đây là lựa chọn bảo thủ cho vòng đời pool hiện tại.

Apply thay DNS policy tạo resolver/pool mới; apply tương thích giữ resolver state.
Mặc định rolling luôn tạo transport pool mới cho worker generation mới;
`reload_mode="hot"` có thể giữ pool tương thích. Xem [rolling-updates.md](rolling-updates.md).
Policy được commit bền vững cùng config; answer/negative cache là RAM state,
khởi tạo lại sau restart. TLS certificate/artifact và xác minh hostname không
bị thay bằng địa chỉ IP vừa resolve.

## Cache xác thực lại và stale khi lỗi

```toml
[upstreams.app.http.cache]
max_bytes = 16777216
max_entries = 1024
max_object_bytes = 262144
max_ttl_seconds = 300
revalidate = true
revalidate_retention_seconds = 300
stale_if_error_seconds = 10
```

Hai chức năng mặc định tắt (`revalidate=false`, stale budget bằng 0). Cache vẫn
chỉ nhận response public có freshness rõ ràng, đủ body và nằm trong budget.
Auth/Cookie/conditional request/range/upgrade và response private/no-store,
Set-Cookie, Vary, trailers, gRPC vẫn bypass theo policy cũ.

Entry hết freshness có ETag hoặc Last-Modified được giữ thêm tối đa
`revalidate_retention_seconds`. Request GET phù hợp gửi If-None-Match, ưu tiên
ETag; nếu không có thì dùng If-Modified-Since. Origin 304 được ghép metadata
với body cũ và trả 200; origin 200 thay representation. ETag khác hoặc thay
content encoding trong 304 bị từ chối. Response đổi sang private/no-store
không giữ representation cũ để phục vụ các request sau.

Revalidation chạy đồng bộ theo request, có backpressure/admission hiện hành.
Không thêm background task cho từng key. Nhiều miss/validation có thể cùng
gọi origin; cache-fill concurrency và memory budget vẫn giới hạn. Đang giữ
lease representation cũ có thể khiến capture mới thiếu budget; khi đó response
vẫn trả được nhưng không đảm bảo lưu lại cache.

Stale chỉ được phép khi **cả cấu hình và origin** cho phép: origin cần
`stale-if-error=N`; thời gian stale không vượt min(N, cấu hình), tính từ lúc
hết freshness, không gia hạn sau mỗi lần lỗi. Áp dụng cho lỗi kết nối/DNS,
timeout chờ response headers, HTTP 500/502/503/504 hoặc không còn endpoint
khỏe. Không fallback cho lỗi upload client, vượt quota, plugin hay lỗi body
sau khi đã gửi headers. Chính sách bảo thủ từ chối stale nếu origin có
must-revalidate, proxy-revalidate hoặc s-maxage.
Fallback giữ lại kết quả lỗi origin cho metrics/passive health; trả body cache
không biến HTTP 503 hoặc lỗi transport của backend thành một lần origin thành công.

Purge vẫn là barrier cho publication: validation bắt đầu trước purge có thể
hoàn tất response đang chạy nhưng không ghi lại entry. Stale fallback kiểm
tra cả epoch và identity entry trước khi lấy body; không lấy lại entry đã bị
purge hoặc thay thế. Lease của response đang gửi tiếp tục được tính budget.

`prostctl cache status` thêm `revalidations` và `stale_hits`; counters và nội
dung cache không được lưu qua restart. Xem [cache-control.md](cache-control.md)
cho key, inspection và purge. Chưa hỗ trợ cache disk, general Vary,
stale-while-revalidate, hoặc request cache-control nâng cao.

Thiết kế đối chiếu [HTTP caching RFC 9111](https://www.rfc-editor.org/rfc/rfc9111.html)
và [stale-if-error RFC 5861](https://www.rfc-editor.org/rfc/rfc5861.html).
Resolver hệ thống tham chiếu [Tokio lookup_host](https://docs.rs/tokio/latest/tokio/net/fn.lookup_host.html).

## Kiểm thử local

```sh
docker run --rm -v "$PWD:/work" -v prost-target:/work/target \
  -v prost-cargo:/usr/local/cargo/registry -w /work \
  -e CARGO_BUILD_JOBS=2 -e RUST_TEST_THREADS=4 prost-dev sh -c \
  'cargo clippy --workspace --all-targets --all-features --locked -- -D warnings && cargo test --workspace --locked && cargo test --workspace --all-features --locked'
python3 scripts/dns-cache-smoke.py --image prost:native \
  --output-dir test-results/dns-cache-smoke-new
```

Smoke tự tạo CA/leaf fixture hai ngày bằng OpenSSL trong output directory.
Có thể truyền --cert-dir với certificate còn hạn, SAN localhost, key.pem/cert.pem
và ca.pem nếu leaf được CA riêng ký.
Chỉ sửa `/etc/hosts` trong container UUID của bài test để đổi answer resolver
hệ thống, không sửa DNS/hosts của máy. Kết quả là kiểm chứng chức năng, không
phải benchmark hay chứng nhận production. Soak image cũ không bao phủ code mới.
