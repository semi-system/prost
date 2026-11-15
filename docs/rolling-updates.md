# Rolling worker khi reload cấu hình

Riêng frontend certificate/SNI/TLS policy có thể dùng `prostctl tls reload`
để giữ worker và phiên đang mở: [tls-refresh.md](tls-refresh.md).
Các ngữ nghĩa drain bên dưới áp dụng full config apply/reload.

Mặc định `reload_mode="rolling"`: parse/validate và chuẩn bị tài nguyên, khởi tạo
thế hệ worker mới, clone socket lắng nghe hiện có, kiểm tra worker readiness,
persist config/artifact, rồi chuyển accept và drain thế hệ cũ.

```toml
[runtime]
reload_mode = "rolling"
io_workers = 2
drain_seconds = 30
```

Đặt `drain_seconds=0` để full rolling không cắt phiên TCP/WebSocket đang mở.
Status trả `drain_unlimited=true` và `drain_remaining_ms=null` cho thế hệ đang
drain vô hạn. Khi vẫn có retiring generation dạng này, full apply tiếp theo trả
`rolling_generation_busy` thay vì giữ coordinator vô hạn. Chờ phiên cũ kết thúc
rồi gửi lại với generation mới nhất. TLS-only reload không tạo thêm worker nên
vẫn được phép. SIGTERM luôn có deadline; với giá trị 0, shutdown dùng 30 giây.

```sh
prostctl validate --file candidate.toml
prostctl reload --file candidate.toml --if-generation 12
# apply vẫn được hỗ trợ, reload là alias của cùng thao tác.
prostctl reload --file candidate.toml --if-generation 12 --wait --wait-timeout 60
# Chờ tiếp sau timeout/mất kết nối, không apply lại:
prostctl workers wait --generation 13 --timeout 60
```

Coordinator serialize validate/apply/patch/GC. Mọi write vẫn kiểm tra expected
generation khi đến lượt thực thi. Hai client gửi cùng generation sẽ có một
request bị generation_conflict; daemon không tự ghi đè hoặc retry candidate cũ.
Validate không activate worker hay bind listener; apply kiểm tra lại và thực hiện
bind/readiness trước commit. Candidate lỗi không dừng worker hiện tại.

Master giữ FD của listener; thế hệ mới đăng ký bản sao của cùng socket/accept
queue lên runtime mới. Không đóng rồi bind lại cổng hiện hành. Upstream client
pool được tạo mới để transport trên runtime cũ không bị dùng bởi worker mới;
state tương thích như cache, native rate/connection table và admission vẫn có
thể dùng chung.

Connection được pin vào snapshot của worker nhận nó. HTTP/1 hoàn tất response
đang chạy rồi đóng keepalive; HTTP/2 graceful shutdown gửi GOAWAY và drain stream
đang chạy, gồm gRPC. Client cần mở connection mới để dùng config mới. TCP và
WebSocket tiếp tục ở thế hệ cũ cho đến khi kết thúc hoặc hết drain_seconds.
Hết hạn thì hủy các phiên còn lại và thu hồi worker. Artifact generation cũ vẫn
được GC bảo vệ khi snapshot còn sống.

Tối đa hai thế hệ worker cùng tồn tại: active và retiring. Reload tiếp theo có
thể chuẩn bị/validate tài nguyên, rồi chờ thế hệ retiring thu hồi trước khi tạo
worker mới. Điều này giới hạn thread/runtime khi có phiên TCP dài. Status trả
`reload_mode`, `worker_generation`, `retiring_worker_generation`; generation
config và số snapshot còn sống vẫn có ý nghĩa riêng.

`io_workers`, worker_local và các runtime setting khác vẫn yêu cầu restart;
`runtime.s3` là ngoại lệ vì chỉ dùng trong staging. Đổi listener identity/protocol
trên cùng địa chỉ hoặc đổi địa chỉ của cùng listener vẫn theo hạn chế hiện có.

Prost hiện rolling **thread/Tokio runtime trong cùng process**, chưa có process
isolation/master-worker process như nginx. Khi worker_local=false, runtime do
caller cung cấp được giữ lại; vẫn thay thế listener generation và drain theo
connection. Vòng đời đối chiếu [nginx reload](https://nginx.org/en/docs/control.html).

Socket HTTP đã accept nhưng chưa nhận request đầu tiên vẫn được đọc request
đầu tiên trước khi graceful drain. Điều này tránh đóng nhầm connection ngay
giữa accept và protocol/header detection. Chờ ban đầu vẫn bị chặn bởi header
timeout (nếu bật) và drain deadline; các keepalive đã xử lý request tiếp tục
được đóng như thường lệ. Quy tắc áp dụng cả khi header timeout được đặt bằng 0.

`reload_mode="hot"` giữ cơ chế trước đây cho tương thích: worker không thay, HTTP
request/H2 stream mới trên connection cũ nhận snapshot mới. Đổi reload_mode cần
restart. Các báo cáo/test cũ về cập nhật trên keepalive mô tả chế độ hot này.

## Chờ hoàn tất và quan sát drain

`reload --wait` commit như thường lệ rồi gọi `reload.wait` trên Unix socket để
đợi các thế hệ **cũ hơn generation vừa commit** thoát và được join. Chờ bằng
completion token, không polling và không giữ coordinator trong lúc drain.
Nếu cấu hình đã tiến lên generation mới hơn, barrier chỉ cần bảo đảm những
worker cũ hơn target đã kết thúc; không giữ lại hoặc khóa generation hiện hành.
Trong chế độ hot, không có worker retiring nên barrier trả ngay.

`--wait-timeout`/`workers wait --timeout` dùng giây, mặc định 60, giới hạn 1–300.
Thời gian chờ gồm cả thời gian lấy coordinator để chụp trạng thái sau commit;
không phải timeout của bước tải artifact/apply. Timeout chỉ kết thúc thao tác
chờ, **không rollback commit**. CLI vẫn in kết quả commit và trả exit khác 0 với
generation đã commit trong lỗi. Kiểm tra status rồi dùng `workers wait` để chờ
tiếp; không gửi lại apply chỉ vì wait bị timeout. `durable=false` vẫn là lỗi vận
hành riêng và vẫn được CLI báo như trước.

Runtime API v1:

```json
{"version":1,"id":"drain-13","method":"reload.wait","generation":13,"timeout_ms":60000}
```

`timeout_ms` giới hạn 1–300000. Generation chưa commit trả
`generation_not_committed`; hết thời gian trả `worker_drain_timeout`. Daemon
cho tối đa tám wait đang chạy, còn lại trả `busy`, giữ chỗ cho status và write
trên giới hạn 32 control session. Đóng socket hoặc shutdown control server hủy
wait mà không thay đổi traffic/config; client `reload.wait` phải giữ cả hai
chiều socket mở và không gửi frame khác trong khi chờ.

`status.worker_generations` có `active`, `retiring` (null nếu đã xong), số
connection/task còn lại, `drain_remaining_ms` và cờ forced/finished. Các tổng
`retired_total`, `forced_total`, `forced_connections_total` tính từ lúc daemon
khởi động, không lưu vào config. Số connection forced là số quan sát tại lúc
quyết định hủy generation, không khẳng định tất cả còn tồn tại khi cancel đến nơi.

Prometheus bổ sung:

| Metric | Ý nghĩa |
|---|---|
| `prost_worker_generations_live` | Active + retiring listener/worker generation, tối đa hai |
| `prost_worker_retiring_connections` | Connection còn giữ thế hệ đang drain |
| `prost_worker_drain_remaining_seconds` | Thời gian còn lại trước force |
| `prost_worker_generations_retired_total` | Generation đã dừng và join |
| `prost_worker_generations_forced_total` | Generation chạm drain deadline |
| `prost_worker_connections_forced_total` | Connection quan sát khi chạm deadline |

Không thêm label generation để tránh cardinality tăng theo mỗi lần reload.
Status/scrape là quan sát đồng thời; dùng barrier khi cần xác nhận một target
đã drain xong.
