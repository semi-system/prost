# Quản trị rate table qua Runtime API

Prost hỗ trợ liệt kê, xem và xóa state của native `rate_limit`,
`connection_rate`, `connection_window` qua Unix socket. Không cần đổi config,
restart worker hoặc ngắt phiên đang chạy. Ví dụ: [rate-tables.toml](../examples/rate-tables.toml).

## CLI

```sh
prostctl rate-table list
prostctl rate-table list --listener web --limit 100
prostctl rate-table show web --route 0 --plugin 0 --limit 100
prostctl rate-table show web --route 0 --plugin 0 --key alice

prostctl rate-table clear web --route 0 --plugin 0 --key alice --if-generation 12
prostctl rate-table clear stream --plugin 0 --key 203.0.113.7 --if-generation 12
prostctl rate-table clear stream --plugin 0 --all --if-generation 12
```

`list` trả `target` gồm listener ID, route index và plugin index để dùng cho
`show`/`clear`. Index bắt đầu từ **0, theo thứ tự khai báo**, không phải thứ tự
priority khi routing. Bỏ `--route` để chọn plugin trực tiếp trên listener.
Route/listener khác có bảng riêng dù plugin cùng tên. Metrics wrapper và hook
filter không làm mất khả năng quản trị bảng.

`clear` bắt buộc chọn đúng một trong `--key` hoặc `--all`; CLI và API đều từ chối
thiếu hoặc truyền cả hai. Generation sai trả `generation_conflict` trước khi
đổi state. Khi bỏ `--if-generation`, CLI đọc generation hiện tại; nếu config đổi
giữa hai request, lệnh vẫn bị từ chối. Nếu dựa trên kết quả `list`/`show` trước
đó, truyền generation từ kết quả đó để bảo vệ selector index khỏi thay đổi.

`--socket /path/admin.sock` áp dụng như các lệnh quản trị khác. Không thêm cổng
quản trị TCP. Không cần bật `runtime.observe_plugins` để sử dụng rate table.

## Khóa và dữ liệu trả về

| Bảng | Kiểu khóa | State mỗi entry |
|---|---|---|
| rate_limit/connection_rate global | text `global` | GCRA debt và retry_after |
| rate_limit/connection_rate theo IP | IP text | GCRA debt, retry_after, idle age |
| rate_limit theo header | bytes, tối đa 256 byte | GCRA debt, retry_after, idle age |
| connection_window | IP text | Ước lượng attempt, limit, window, idle age |

IPv4-mapped IPv6 và IPv4 dùng chung IP key, cả khi admission và khi quản trị.
IP phải là địa chỉ đơn, không phải CIDR/socket address. Khóa global chỉ nhận
text `global`. `debt_ms` là thời gian tới GCRA virtual next timestamp;
`retry_after_ms` tính cả burst allowance, có thể bằng 0 khi vẫn còn burst.
Đây không phải tổng số request. Window đếm cả attempt bị window từ chối;
`estimated_attempts` giảm theo thời gian ngay cả khi không có traffic mới.

Khóa header trong kết quả luôn dùng hex, kể cả header UTF-8 thông thường,
để bảo toàn byte và tránh nhầm khóa không giải mã được. Header không có hoặc
giá trị rỗng dùng chung khóa bytes rỗng. Ví dụ `alice` tương ứng `616c696365`:

```sh
prostctl rate-table show web --route 0 --plugin 0 --key 616c696365 --key-hex
prostctl rate-table clear web --route 0 --plugin 0 \
  --key 616c696365 --key-hex --if-generation 12
```

`list`/`show` mặc định tối đa 100 hàng, cho phép `--limit 1..1024`, trả cờ
`truncated` nếu còn dữ liệu phù hợp. `show --key` tra chính xác một khóa;
`retained_entries` vẫn là số entry đang giữ trong toàn bảng. `show` không
admit, refresh TTL hoặc chạy GC. Entry hết hạn có thể còn trong bảng đến lần
lazy cleanup tiếp theo của traffic.

Đọc khóa một IP/header không làm lộ các khóa khác. Danh sách entry không có
thứ tự ổn định/pagination cursor; đây là mẫu có giới hạn, không phải export toàn
bộ state. Có thể lọc `list --listener` và tra `show --key` khi bảng lớn.

## Semantics khi traffic tiếp tục chạy

Limiter state **ephemeral**: xóa state không serialize lại file, không tăng
config generation và không persist counter qua restart. Response clear trả
`ephemeral=true`, `config_changed=false`, `affected_entries`. Đây là số entry
đã reset, không cam kết số entry vật lý đã bị remove. Global bucket luôn có
một entry. Config vẫn giữ rate/window/burst đã khai báo.

Clear serialize với apply qua coordinator. Sau khi đã nhận và bắt đầu dispatch,
thao tác hoàn tất dù client quản trị mất kết nối. CLI không tự retry lệnh khi
mất response: traffic có thể đã phát sinh state mới sau clear, nên cần inspect
lại trước khi quyết định xóa lần nữa.

Mỗi shard được đọc/xóa riêng, không dừng traffic toàn cục. `atomic_snapshot=false`
trong view; tổng occupancy và entry không phải một ảnh chụp nguyên tử của mọi
shard. Clear-all cũng không phải barrier cho các connection/request mới: khóa
có thể xuất hiện lại ngay. Một bucket GCRA còn in-flight được reset tại chỗ và
giữ cùng identity để request cũ/mới không có hai budget độc lập sau clear.

Config/plugin không đổi giữ cùng bảng qua hot apply. Thay cấu hình plugin tạo
state mới theo cơ chế chuẩn bị plugin hiện tại. Bảng gắn với snapshot cũ còn
sống không tự được đổi thành bảng mới nếu plugin đã thay đổi. API chỉ chọn
plugin trong snapshot hiện hành; read trả generation/hash của snapshot đó.
Generation trong response envelope khớp generation của kết quả được đọc.

Không xóa active connection semaphore, per-IP active count, health state hoặc
cumulative metrics bằng các lệnh này. `connection_limit` concurrent không phải
rate table. Phiên TCP/WebSocket/gRPC đã được admission tiếp tục hoạt động.

## JSON Runtime API v1

Request vẫn dùng JSON có length prefix và envelope v1 hiện có. Ví dụ payload:

```json
{"version":1,"id":"tables","method":"rate_table.list","limit":100}
```

```json
{"version":1,"id":"inspect","method":"rate_table.show","target":{"listener":"web","route":0,"plugin":0},"query":{"key":{"value":"alice","encoding":"text"},"limit":100}}
```

```json
{"version":1,"id":"clear-ip","method":"rate_table.clear","target":{"listener":"stream","plugin":0},"key":{"value":"203.0.113.7","encoding":"text"},"expected_generation":12}
```

```json
{"version":1,"id":"clear-all","method":"rate_table.clear","target":{"listener":"stream","plugin":0},"all":true,"expected_generation":12}
```

`list.listener` tùy chọn; `show.query` mặc định không lọc, limit 100.
`clear.expected_generation` bắt buộc. Invalid listener/route/plugin, plugin
không có bảng, khóa sai encoding/định dạng hoặc limit sai đều trả lỗi.
Response tối đa 4 MiB như API hiện tại. `RunningProxy` cũng cung cấp
`rate_table_list`, `rate_table_show`, `rate_table_clear` để nhúng Rust;
native filter tùy chỉnh có thể triển khai trait `prost_plugin::rate_table::RateTable`.

## Kiểm chứng

Build/test local Docker trong WSL, Rust 1.93, Cargo.lock; không benchmark/PGO
trong phần này. Fmt/clippy all-targets/all-features sạch; **163 test đạt ở mỗi
chế độ native và all-features**. Một crash fixture được test cha gọi bằng
subprocess như trước. Các ca mới gồm đọc không đổi state, canonical IP, hex
không UTF-8, clear không tách budget in-flight, route/H2/hot apply, TCP phiên cũ,
generation conflict, thiếu generation, client disconnect và CLI scope/bounds.

Release image đã kiểm tra:
`sha256:760e7022560d6a9efc0ff1d0d13597e53a3c4a0febb78854ec8c4c82e56f4930`.
`scripts/rate-table-smoke.py` dùng CLI thật: HTTP 429/TCP window rejection có
chủ đích, discovery/show, clear hex/IP/all, tự đọc generation, từ chối scope
sai/conflict, giữ TCP qua apply/clear, restart giữ config nhưng reset bảng.
Raw: `test-results/rate-table-final-smoke-v2/summary.json` và `commands.json`.
Lượt đầu chỉ lỗi harness do tên biến che module `http`, trước request đầu;
đã sửa và giữ log trong `rate-table-final-smoke`, không tính lượt đó là đạt.

```sh
sudo python3 scripts/rate-table-smoke.py --image prost:native \
  --output-dir test-results/rate-table-smoke-new
```

Soak `test-results/soak-24h-edge` vẫn chạy bằng immutable image **89da… trước
thay đổi này**. Nó không chứng minh image 760e… đã qua 24 giờ. Ưu tiên hiện tại
là chức năng; điều tra/tối ưu performance tiếp tục để sau.
