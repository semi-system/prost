# Runtime API quản trị RAM cache

Xem status, liệt kê entry, tính key và purge cache qua Unix socket/CLI. Ví dụ bật
cache cho upstream: [http-cache.toml](../examples/http-cache.toml). Chính sách cache
public/TTL/bypass hiện có vẫn áp dụng; xem [mvp-bundle.md](mvp-bundle.md).
TTL/negative DNS, ETag/Last-Modified và stale-on-error:
[dns-cache.md](dns-cache.md).

## CLI

```sh
prostctl cache status
prostctl cache status app
prostctl cache entries app --limit 100
prostctl cache key --uri '/cached/item?q=1' --host public.example \
  --accept-encoding identity
prostctl cache entries app --key <64-hex-digits>
prostctl cache purge app --key <64-hex-digits> --if-generation 12
prostctl cache purge app --all --if-generation 12
```

Upstream là phạm vi cache, dùng chung cho các endpoint trong upstream đó.
`--all` xóa mọi entry **của upstream được chọn**, không phải mọi cache trong
daemon. `purge` bắt buộc chọn đúng một trong `--key`/`--all`, cả CLI lẫn API
đều kiểm tra. Generation sai bị từ chối trước mutation; bỏ --if-generation thì
CLI đọc generation hiện tại rồi gửi điều kiện. Unknown upstream, cache tắt,
key sai hoặc limit ngoài phạm vi đều trả lỗi. Không thêm cổng admin TCP.

`status` không chỉ định upstream trả các upstream hiện có; cache chưa bật có
enabled=false. `entries` yêu cầu cache bật, mặc định tối đa 100 entry, hỗ trợ
1..1024 và cờ truncated. Danh sách là mẫu có giới hạn, không có thứ tự ổn định
hoặc cursor export toàn bảng. Tra --key để lấy đúng một entry.

## Key và biến thể request

Key là SHA-256 64 hex digits theo thuật toán lookup hiện có: URI được HTTP
parser giữ lại, Host và từng occurrence Accept-Encoding theo thứ tự. Không
tự lowercase Host, gộp occurrence, sort encoding hoặc bỏ query. Hash không
chứa tên upstream; hai upstream có cache riêng dù cùng key. Output dùng hex
chữ thường, query/purge chấp nhận cả chữ hoa.

`cache key` tính fingerprint cho request GET rỗng. Đây là công cụ dựng key,
không khẳng định request/response thực tế đủ điều kiện được cache.

- HTTP/1 origin-form thường dùng --uri '/cached/item' và --host đúng giá trị.
- HTTP/2 thường có URI absolute-form, ví dụ --uri 'https://public.example/cached/item';
  bỏ --host nếu không có header Host riêng. Authority không tự được thêm thành Host.
- Cung cấp URI/Host/Accept-Encoding **sau native request rewrite/header filter**,
  trước khi Prost dựng URI kết nối backend. Dùng authority backend để tính key
  có thể không khớp request đầu vào.
- Lặp --accept-encoding cho nhiều occurrence; 'gzip, br' trong một field khác
  hai field 'gzip' và 'br'. Một số client tự gửi identity; cần dùng đúng giá trị.

URI key input hỗ trợ origin-form và absolute HTTP(S), tối đa 32 KiB; Host tối
đa 32 KiB; Accept-Encoding tối đa 16 occurrence và 32 KiB tổng. Header value
phải hợp lệ. API key input là text UTF-8; entry có biến thể bytes khác vẫn có
thể xem/purge bằng hash lấy từ entries. Không lưu thêm URL/header request dạng
plaintext trong mỗi entry để phục vụ quản trị.

## Status và memory accounting

Status có entries, used_bytes, active_captures, limits, hits/misses/evictions,
epoch, purges, purged_entries và discarded_fills. Entry có key, body_bytes,
reservation_bytes, age_seconds, remaining_ttl_ms, expired. Inspection không
lookup traffic, tăng hit/miss, refresh TTL hoặc chạy GC. Entry hết TTL có thể
còn trong map đến lần lazy cleanup tiếp theo.

used_bytes là **budget reservation của cache**, không phải RSS/heap đo thực tế.
Nó gồm entry, capture và payload cache đang được response giữ. Purge bỏ entry
khỏi lookup; Bytes clone/slice đang được client hoặc H2 queue giữ vẫn giữ memory
reservation. Vì vậy entries=0 chưa buộc used_bytes=0. Memory được trả khi hết
tham chiếu; purge không hủy response đang gửi chỉ để thu hồi memory.

View entries/epoch được đọc dưới khóa cache. Các byte reservation có thể được
thả đồng thời ngoài khóa; status nhiều upstream cũng không phải một snapshot
nguyên tử của mọi traffic. Counter không bị reset bởi purge; purges tách riêng
khỏi eviction tự động.

## Purge trong lúc đang tải response

Mỗi miss giữ epoch trước khi bắt đầu gọi origin. Purge tăng epoch dưới cùng
khóa với lookup/publication rồi xóa key hoặc map đã chọn. Capture kiểm tra epoch
ở lúc nhận headers, append body và finish. Một fill có epoch cũ không thể
publish lại entry sau purge, kể cả response còn chờ headers hoặc đã nhận DATA
cuối nhưng chưa nhận EOS. Fill cũ không được xóa/ghi đè entry mới của cùng key.

Epoch áp dụng cho cả cache instance: **purge một key cũng vô hiệu hóa mọi fill
đang chạy từ trước purge trong cache đó**, một cách bảo thủ. Entry của key khác
đã hoàn tất vẫn được giữ. Không duy trì tombstone vô hạn theo key. Purge key
chưa có trong map vẫn tăng epoch vì key đó có thể đang được fetch.

Response đã bắt đầu vẫn tiếp tục gửi đủ dữ liệu. Capture bị vô hiệu hóa được
thả khi tiếp tục poll/finish/drop; discarded_fills đếm các lần từ chối publication/
capture do epoch cũ, không phải số request client bị lỗi. Request bắt đầu lookup
sau purge có thể tạo entry mới ngay. Đây là invalidation barrier cho fill cũ,
không phải chế độ cấm cache liên tục.

Purge serialize với apply qua coordinator và hoàn tất thao tác đã dispatch dù
client quản trị mất kết nối. Response trả generation, purge.epoch,
removed_entries, used_bytes_after, active_captures, ephemeral=true và
config_changed=false. CLI không tự retry mutation khi mất response; inspect
state trước khi quyết định purge lại.

Purge không tăng config generation hoặc ghi lại file config. Generation bảo vệ
identity cấu hình/cache, không khóa tập entry traffic. Apply tương thích giữ
cùng cache/epoch; thay policy hoặc transport không tương thích có thể tạo cache
mới. API chọn cache của snapshot hiện hành, không truy cập cache riêng của một
generation cũ đã bị thay thế. Response cũ vẫn có thể giữ dữ liệu đã lấy trước đó.
Epoch/counter/content là RAM state, bắt đầu lại từ 0/rỗng sau restart; config đã
commit vẫn giữ nguyên.

## JSON Runtime API v1 và Rust API

Request dùng envelope v1 và JSON length prefix hiện có. Ví dụ payload:

```json
{"version":1,"id":"status","method":"cache.status","upstream":"app"}
```

```json
{"version":1,"id":"entries","method":"cache.entries","upstream":"app","query":{"limit":100}}
```

```json
{"version":1,"id":"key","method":"cache.key","input":{"uri":"/cached/item","host":"public.example","accept_encoding":["identity"]}}
```

```json
{"version":1,"id":"purge","method":"cache.purge","upstream":"app","all":true,"expected_generation":12}
```

Purge một key thay all=true bằng key chứa 64 hex digits; expected_generation
bắt buộc. `status.upstream` tùy chọn, `entries.query` mặc định limit 100.
Response giữ frame limit 4 MiB hiện có và generation khớp snapshot kết quả.

`RunningProxy` có cache_status/cache_entries/cache_purge; cache_key và
CacheKeyInput/CacheQuery được re-export cho embedder. `HttpResources` cung cấp
status/entries/purge trực tiếp cho ứng dụng nhúng không dùng CLI.

## Kiểm chứng local Docker/WSL

Fmt/clippy all-targets/all-features sạch, **182 test đạt ở mỗi chế độ native và
all-features**. Log: `test-results/cache-control-checks.log`. Các ca mới kiểm tra
purge khi chờ headers/đang DATA/chờ EOS, fill cũ không ghi đè fill mới, Bytes slice
giữ budget sau purge, exact key/encoding variant, read-only inspection, only-if-cached
504 không gọi origin, hot apply/persistence/restart, API missing/conflicting scope,
generation conflict, client disconnect và apply/purge đồng thời.

Image release native, Rust 1.93/Cargo.lock:
`sha256:bb495a774732e2b0562bd373ea3ec2feb571e8c735e8624a477266e227157848`.
CLI smoke dùng counter của backend Go độc lập để chứng minh hit/miss, tính key
đúng, purge một key giữ biến thể khác, purge-all, generation lookup/conflict,
hot apply và restart giữ cấu hình. Raw:
`test-results/cache-control-final/summary.json` và commands.json.
Các race streaming được kiểm tra bằng barrier ở integration test, không suy ra
từ counter của smoke đơn giản.

```sh
sudo python3 scripts/cache-control-smoke.py --image prost:native \
  --output-dir test-results/cache-control-new
```

Đợt này hoàn thiện quản trị RAM cache; chưa thêm purge theo path/prefix, revalidation,
stale serving hoặc disk cache. Không chạy benchmark. Soak đang chạy ở
`test-results/soak-24h-edge` vẫn dùng immutable image 89da… trước các chức năng
mới; nó không chứng minh image bb49… đã qua 24 giờ.
