# Quản lý artifact và kiểm thử persistence

Daemon có GC certificate, private key, CA và WASM qua Runtime API. GC chạy khi được gọi, không có polling hoặc watcher. Cấu hình và traffic tiếp tục dùng snapshot hiện tại; việc đọc thư mục/xóa/fsync chạy trên blocking pool, được coordinator serialize với apply, patch và validate.

```sh
# Mặc định chỉ xem trước; chỉ xét file không được tham chiếu và cũ ít nhất 1 giờ.
prostctl artifacts gc

# Xóa với generation đã kiểm tra; không tăng generation của cấu hình.
prostctl artifacts gc --delete --if-generation 12

# Cho phép dọn artifact mới, giới hạn công việc trong một lượt.
prostctl artifacts gc --delete --min-age-seconds 0 \
  --max-scan-entries 100000 --max-delete-files 1024 --if-generation 12

prostctl status
```

CLI tự đọc generation nếu không truyền `--if-generation`; server vẫn kiểm tra khi vào coordinator. Preview không đặt trước kết quả xóa: một apply giữa preview và collect có thể đổi tập tham chiếu hoặc gây generation conflict. API v1 có method `artifacts.gc`, trường `expected_generation` bắt buộc; `dry_run` mặc định `true`, `min_age_seconds=3600`, `max_entries=100000`, `max_delete_files=1024`.

**Tập được giữ lại:**

- Artifact của cấu hình hiện tại và mọi published snapshot còn sống. GC giữ strong reference tới các snapshot đó cho đến khi hoàn tất lượt dọn.
- Cấu hình trên đĩa phải có hash khớp snapshot đang chạy. Nếu bị sửa trực tiếp, GC trả `managed_config_changed` để không xóa file mà cấu hình khởi động có thể cần.
- Trước khi xóa, GC fsync lại config và thư mục chứa config. Điều này xác nhận durability trước khi thu hồi file của một cấu hình cũ sau trường hợp commit từng trả `durable:false`.
- Apply/validate đang chuẩn bị không chạy đồng thời với GC. Cả validate lẫn thao tác ghi đều hoàn tất coordinator task đã bắt đầu dù caller bị hủy; disconnect không để blocking staging tiếp tục ngoài khóa.

Snapshot lưu sẵn tập hash ở lúc prepare. Không thêm lookup artifact, lock GC hoặc kiểm tra file vào từng request. `status.artifact_gc` chứa report gần nhất trả về thành công, kể cả report có lỗi một phần; lỗi trước scan được trả trong API và tính vào counter lỗi.

**Quy tắc dọn:** chỉ unlink regular file tên SHA-256 lowercase 64 ký tự và temp artifact theo mẫu do Prost tạo, nằm trực tiếp trong kho. Không duyệt thư mục con, không theo symlink và không xóa file tên lạ. Tuổi file tính từ mtime; file có timestamp ở tương lai được bỏ qua. `min_age_seconds` tối đa một năm; scan tối đa một triệu entries, xóa tối đa 100.000 file/lượt. Defaults nhỏ hơn giới hạn này.

Report có `referenced_files/bytes`, `eligible_files/bytes`, `deleted_files/bytes`, `skipped_entries`, `errors`, `first_error`, `durable`, `scan_limited`, `delete_limited`, generation và số generation còn sống. `eligible` chưa chắc đều được xóa vì budget/IO error. Nếu scan chạm giới hạn, tăng budget thích hợp và gọi lại; không có cursor bền vững. Preview tính eligible nhưng không unlink. Khi có lỗi từng file, collect tiếp tục và báo lỗi hữu hạn; CLI in report rồi trả exit code khác 0 nếu `errors>0` hoặc không durable. Cờ limited tự nó không phải IO error.

## Quyền sở hữu và filesystem

Một kho artifact chỉ do **một daemon** quản lý. Daemon giữ `.prost-store.lock` độc quyền trong suốt vòng đời; chạy nhiều instance phải dùng `runtime.artifact_dir` riêng. Thư mục dùng mode `0700`, lock và artifact mới dùng `0600`. GC bám directory FD trên Linux để không bị đổi đường dẫn sang thư mục khác khi unlink; root và lock bị thay thế sẽ bị phát hiện, yêu cầu restart.

File nguồn PEM/CA có thể là symlink thông thường, chẳng hạn cách tổ chức certificate bên ngoài kho. File bên trong kho được đọc không theo symlink và phải khớp hash trong tên; corruption không được tự nhập lại thành artifact mới. Nguồn artifact được kiểm tra regular file và kích thước trước khi đọc, đồng thời chặn đọc quá 16 MiB nếu file tăng trong lúc đọc.

GC thực hiện unlink, không hứa xóa dữ liệu vật lý khỏi SSD/backup. Kho là dữ liệu do daemon quản lý; không dùng nó như thư mục lưu file tùy ý. File temp của **config** ở ngoài kho artifact không thuộc lượt dọn này. File temp mới có nonce ngẫu nhiên, nên temp từ process crash/PID cũ không chặn restart. Chỉ temp do chính lần ghi đó tạo mới được cleanup khi ghi thất bại.

## Commit và fault tests

Atomic rename vẫn là commit point. Lỗi trước rename giữ nguyên runtime và config cũ. Lỗi directory fsync sau rename phải publish snapshot mới và trả `durable:false`; không được báo như config cũ còn có thẩm quyền. CLI in kết quả rồi trả exit code khác 0 khi không durable: cần đọc generation/hash trước khi thử lại. Bản TOML serialize cũng phải nằm trong giới hạn đọc startup 3 MiB, kể cả khi formatting/defaults làm nó lớn hơn input.

Kiểm thử Rust gồm lỗi ENOSPC được inject ở create/write/file-fsync/rename, lỗi fsync sau rename, và child process thực sự thoát trước rename, sau rename và sau directory fsync trước publish. Restart kiểm tra đúng generation trên đĩa. Hook chỉ nằm trong test build; không có biến môi trường failpoint trong daemon phát hành. Đây là process-crash tests, không mô phỏng mất điện hoặc rollback của disk controller.

Test TLS giữ response stream cũ qua certificate rotation, chạy GC, chỉ dọn key/cert cũ khi generation cũ hết tham chiếu, rồi restart khi source PEM đã bị xóa. Các test khác bao phủ orphan từ validate, age/budget, symlink, thư mục bị thay thế, corruption, store dùng chung, generation conflict và client disconnect.

## Metrics và kiểm thử CLI dưới tải

- `prost_artifact_gc_runs_total{mode="dry_run|collect",result="success|partial|error"}`.
- `prost_artifact_gc_deleted_files_total`, `prost_artifact_gc_deleted_bytes_total`: unlink thành công, không phải số byte vật lý đã trả cho filesystem nếu còn hard link/open FD.
- `prost_artifact_gc_seconds`: gồm thời gian chờ coordinator. Scrape không tự quét artifact store.

Chạy local trong WSL sau khi build image và chuẩn bị backend/fixtures:

```sh
sudo python3 scripts/artifact-smoke.py --image prost:native
```

Harness dùng container/network riêng: stream 10.000 HTTPS request, 10 lần apply và GC, dọn 100 orphan, kiểm tra metrics/generation rồi restart. Thêm một daemon trên tmpfs **64 KiB** để gây ENOSPC thật bằng artifact 128 KiB: apply phải thất bại, không để lại partial artifact, generation cũ và TLS vẫn phục vụ. Không làm đầy đĩa host. Raw traffic/status/resources nằm ở `test-results/artifact-smoke/`. Đây là test correctness và tài nguyên, không lấy RPS trong lúc fault/GC làm benchmark hiệu năng.
