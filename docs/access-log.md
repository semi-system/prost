# Native access log và metrics transport upstream

Container có thể gửi access/session log trực tiếp tới stdout:

```toml
[runtime.access_log]
sink = "stdout"
format = "json"
queue_capacity = 2048
```

Không khai báo `path` khi dùng stdout; sink file vẫn là mặc định và yêu cầu path.
Stdout dùng writer thread, bounded queue, sampling và cùng bộ đếm drop/IO; không
đọc `/dev/stdout` hoặc yêu cầu filesystem writable cho log. Flush/shutdown giữ
ngữ nghĩa hiện tại; reopen flush rồi thay handle stdout. Collector cần tiêu thụ
stdout; nếu bị nghẽn writer có thể chờ I/O và queue đầy sẽ drop record theo counter,
không tăng queue vô hạn. Log vận hành JSON và access log có thể cùng stdout.
Đổi sink thuộc runtime config, cần restart. Quyền file/rotate bên dưới dành cho
sink file.

Bản mở rộng có format compact, status/path filter và service logging riêng:
[mvp-bundle.md](mvp-bundle.md). Các ngữ nghĩa JSON đầy đủ bên dưới vẫn giữ nguyên.

Access log JSON Lines cho HTTP/1.1, HTTP/2, gRPC, WebSocket và TCP/TLS. Mặc định
tắt, không cần WASM. Ví dụ chạy được: [examples/access-log.toml](../examples/access-log.toml).

```toml
[runtime.access_log]
path = "/var/lib/prost/access.jsonl"
queue_capacity = 2048
sample_every = 1
include_query = false
max_path_bytes = 1024
flush_interval_ms = 1000
```

Daemon cần quyền ghi thư mục đã tồn tại. File mới dùng mode `0600`; file đã có
giữ mode hiện tại. Chỉ nhận regular file, từ chối symlink, hard link, config/lock
và đường dẫn trong kho artifact. Chưa hỗ trợ stdout, syslog hoặc remote sink.
Các settings trong `runtime.access_log` cần restart; sửa file không tự cập nhật.

`queue_capacity` cho phép 1–65536 record, `sample_every` 1–1000000,
`max_path_bytes` 8–4096 và `flush_interval_ms` 10–60000. Sampling dùng bộ đếm
chung: chọn 1 trong N request/session đủ điều kiện, không phải ngẫu nhiên.
Listener và route có thể bật/tắt qua `apply` mà không restart:

```toml
[[listeners]]
id = "http"
address = "0.0.0.0:8080"
protocol = "http"
upstream = "web"
access_log = false

[[listeners.routes]]
path_prefix = "/api"
upstream = "web"
access_log = true
```

Listener mặc định `true`; route kế thừa nếu không khai báo. Khi sink chưa được
cấu hình thì các flag không tạo log. Route chọn theo URI gốc trước rewrite.
Request/stream HTTP mới lấy flag và generation mới; TCP/WebSocket/stream đang
chạy giữ ticket cũ đến khi kết thúc. Request chưa parse thành công, TLS HTTP
handshake lỗi và connection bị từ chối trước admission không có HTTP access log.

## Record và thời điểm ghi

Các trường cố định:

| Trường | Ý nghĩa |
|---|---|
| `schema_version`, `timestamp_unix_ms` | Schema 1, thời điểm bắt đầu quan sát |
| `protocol`, `http_version`, `method` | `http`/`websocket`/`tcp`; trường HTTP null với TCP |
| `peer`, `listener`, `generation` | Peer socket thực tế, listener và snapshot |
| `path`, `truncated` | URI gốc đã giới hạn độ dài UTF-8; query mặc định bỏ |
| `status` | Status sau response filter; null với TCP hoặc không có headers |
| `upstream`, `endpoint` | Upstream/address đã chọn; null nếu bị chặn trước bước chọn |
| `headers_us`, `duration_us` | Thời gian đến response headers và kết thúc, microsecond |
| `body_bytes`, `outcome` | Byte quan sát và `complete`/`error`/`aborted` |

Không ghi request/response headers, cookie, authorization hoặc body. Đường dẫn
vẫn có thể chứa dữ liệu nhạy cảm; `include_query=true` chủ động ghi cả query.
Chuỗi được JSON escape, giới hạn kích thước; `truncated` cho biết path/method bị cắt.

HTTP ghi khi body kết thúc, gặp lỗi hoặc bị hủy, bao gồm trailer. HEAD/1xx/204/304
ghi 0 byte ngay khi có response. `body_bytes` đếm DATA giao cho downstream HTTP
driver, không gồm header/trailer, framing/TLS và không chứng minh client đã nhận.
Lỗi body sau headers vẫn giữ status đã gửi, ví dụ `200` cùng `outcome="error"`.
HTTP bắt đầu đo khi service nhận request đã parse headers; không gồm TLS
handshake hoặc thời gian đọc headers trước đó. TCP bắt đầu đo sau admission,
trước handshake nếu listener có TLS. Đây chưa phải format/timing tương thích
hoàn toàn với access log nginx.

WebSocket ghi khi tunnel kết thúc, status 101, byte relay hai chiều bao gồm WS
framing. TCP cũng đếm tổng relay hai chiều và giữ half-close. Với relay lỗi có
thể không có số byte, khi đó `body_bytes=null`. TLS ngoài relay không được tính.
`aborted` dùng khi stream/task bị hủy hoặc chưa có kết quả kết thúc xác định.
Mỗi ticket phát tối đa một record.

## Queue, lỗi ghi và rotate

Một writer thread dùng buffer 64 KiB và serialize JSON ngoài worker traffic.
Worker chỉ enqueue không chờ vào queue hữu hạn; queue đầy thì bỏ record và tăng
counter. Ticket/body observer chỉ được tạo cho request được chọn khi log bật.
Active ticket chịu giới hạn connection/request hiện có, không có queue vô hạn.

Nếu ghi/flush lỗi, writer bỏ buffer và ngừng ghi đến khi quản trị gọi `reopen`.
Traffic tiếp tục; record mới trong thời gian đó được tính `io_uncertain`. Nhóm
này là record mất hoặc chưa xác định đã ghi được bao nhiêu, không phải số dòng
chắc chắn mất: một phần buffer có thể đã xuống file trước lỗi. File có thể có
dòng JSON cuối chưa hoàn chỉnh sau lỗi I/O/crash; hãy rotate file đó hoặc sửa tail
trước khi dùng lại. Logger không tự retry gây trùng dòng.

```sh
prostctl access-log flush --if-generation 12
mv /var/lib/prost/access.jsonl /var/lib/prost/access.jsonl.1
prostctl access-log reopen --if-generation 12
```

`flush` là fence cho record enqueue trước lệnh; không chờ request đang chạy và
không `fsync`. `reopen` mở trước file mới, flush file cũ nếu có thể rồi chuyển FD.
Mở file mới thất bại giữ sink cũ; flush file cũ thất bại vẫn có thể chuyển sang
file mới, counter phản ánh lỗi. Không polling file hoặc tự phát hiện rotate.

Runtime API v1: `access_log.flush`/`access_log.reopen`, đều yêu cầu
`expected_generation`; CLI tự đọc generation khi bỏ `--if-generation`.
Lệnh được coordinator serialize, không tăng generation và không sửa config.
Response có `epoch` tăng sau mỗi reopen. Mất response: đọc status `reopens`
trước khi retry. Chờ queue/writer có timeout 5 giây; việc mở file trên filesystem
bị treo không có đảm bảo thời gian tuyệt đối. SIGTERM drain traffic rồi flush;
crash/power loss có thể mất buffer/queue. Đây không phải audit log bền vững.

`prostctl status` có `access_log`; Prometheus có:

- `prost_access_log_enabled`, `prost_access_log_writer_alive`.
- `prost_access_log_records_accepted_total`, `prost_access_log_records_processed_total`
  (dequeue, không đảm bảo xuống đĩa), `prost_access_log_records_sampled_out_total`.
- `prost_access_log_records_pending`, `prost_access_log_observations_active`.
- `prost_access_log_records_dropped_total{reason="queue_full|io_uncertain|closed"}`.
- `prost_access_log_io_errors_total`, `prost_access_log_reopens_total`,
  `prost_access_log_flushes_total` (không fsync).

## Kết nối upstream thực tế

Các family dưới đây có nhãn `upstream`, đo transport HTTP client thực tế, độc lập
với gauge nhóm pool. Không gồm health probe hoặc raw TCP upstream.

| Metric | Ý nghĩa |
|---|---|
| `prost_http_upstream_connect_attempts_total` | Lượt gọi connector |
| `prost_http_upstream_connecting` | DNS/TCP/TLS đang chờ |
| `prost_http_upstream_connect_total{result}` | `success`, `error`, `timeout`, `cancelled` |
| `prost_http_upstream_connections_open` | Transport còn sống, gồm idle, active, upgrade, pool cũ đang drain |
| `prost_http_upstream_connections_opened_total`, `prost_http_upstream_connections_closed_total` | Transport tạo thành công và đã drop |
| `prost_http_upstream_connect_seconds_total` | Tổng thời gian attempt, gồm DNS/TCP/TLS và cancellation |
| `prost_http_upstream_tls_connect_attempts_total`, `prost_http_upstream_tls_connect_success_total`, `prost_http_upstream_tls_connect_seconds_total` | Nhánh HTTPS; duration gồm cả DNS/TCP, không phải TLS handshake riêng |

Counter/lease giữ theo tên upstream qua thay pool/TLS/protocol; stream generation
cũ cùng tên vẫn cập nhật. Xóa upstream thì series không còn export; thêm lại hoặc
restart có thể reset. `open` giảm theo lifetime transport, không theo request
hay theo lần thay snapshot. Scrape các counter không phải snapshot nguyên tử.

Theo dõi reuse là tùy chọn để tránh thêm atomic/metadata vào mỗi response mặc định:

```toml
[upstreams.web]
observe_connection_reuse = true
```

`prost_http_upstream_reuse_tracking` báo 0/1.
`prost_http_upstream_connection_responses_total{usage="first|reused"}` đếm
response headers đầu tiên và tiếp theo trên cùng transport. HTTP/2 multiplexing
cũng dùng cách đếm này. Counter không tăng cho attempt không nhận response.
Khi tắt tracking, request mới không tăng reuse counter; request generation cũ
đang chờ headers vẫn có thể hoàn tất việc đếm. Transport counters vẫn chạy. Đây không
phải pool hit/miss và chưa phân biệt socket idle/busy bên trong Hyper.

## Kiểm thử local

```sh
sudo python3 scripts/access-log-smoke.py --image prost:native
```

Script tạo network/container có tên riêng, 2 CPU và RAM 256 MiB, ép đầy tmpfs log
64 KiB trong 10000 request H2. Kiểm tra byte count, queue hữu hạn, writer sống,
flush báo lỗi, truncate/reopen phục hồi, JSON/query, rotate, generation conflict,
hot disable, restart và metrics transport. Kết quả ở `test-results/access-log-smoke`.
Đây là kiểm tra correctness/resource, không phải benchmark throughput.

Số đo cụ thể, chi phí bật log và giới hạn benchmark:
[access-log-benchmark.md](access-log-benchmark.md).
