# Static files: disk cache và preload RAM

Route HTTP có thể phục vụ file trực tiếp, không cần upstream. Có GET/HEAD,
index theo thứ tự, MIME, ETag/Last-Modified, conditional request và một byte
range (206/416). Range nhiều phần hoặc đơn vị khác được bỏ qua, trả toàn file.
Không bật directory listing. Native ACL/header/rewrite/rate-limit và access log
vẫn đi qua pipeline thông thường; không chạy hook chọn upstream cho route local.

Ví dụ đầy đủ: [static-files.toml](../examples/static-files.toml).

```toml
schema_version = 1
upstreams = {}

[runtime]
max_static_ram_bytes = 268435456 # 256 MiB, toàn proxy; thay đổi cần restart

[[listeners]]
id = "web"
address = "0.0.0.0:8080"
protocol = "http"

[[listeners.routes]]
path_prefix = "/assets/"
# Không khai báo upstream trên route static.
[listeners.routes.static_files]
root = "/srv/www"
strip_prefix = true
index = ["index.html"]
load_to_ram = false
max_open_files = 256
max_ram_bytes = 67108864       # 64 MiB cho cache/preload của route
max_ram_files = 10000
cache_max_file_bytes = 1048576 # disk cache: tối đa 1 MiB/file; 0 để tắt
ram_headroom_bytes = 67108864  # chừa thêm 64 MiB ngoài dung lượng cần nạp
```

`/assets/a.css` ánh xạ vào `/srv/www/a.css` khi `strip_prefix=true`.
Mặc định `strip_prefix=false`: đường dẫn đầy đủ được nối vào root.
Thư mục thiếu slash cuối trả 301 và giữ query. Chỉ đọc regular file; mọi thành
phần được mở từ directory FD bằng `openat/O_NOFOLLOW`, chặn traversal và symlink
kể cả khi đường dẫn thay đổi giữa kiểm tra và mở file. Root cuối cũng không
được là symlink. Các tên index phải là tên file đơn, không có `/` hoặc `..`.

## Disk là mặc định, vẫn có cache

`load_to_ram=false` không nạp cây file lúc validate/startup. Khi được truy cập,
file nhỏ được cache RAM theo nhu cầu. Cache có giới hạn byte, số entry và
singleflight cho mỗi file đang nạp; eviction FIFO. File lớn hơn
`cache_max_file_bytes` được stream từ disk bằng chunk 16 KiB. Linux page cache
vẫn hoạt động bình thường cả khi tắt cache ứng dụng.

Mỗi request mở file theo root FD và kiểm tra inode, size, mtime/ctime nanosecond.
Disk mode mở lại root tại mỗi lần truy cập, nên deploy bằng atomic rename cả
thư mục cũng nhìn thấy cây mới mà không giữ FD trỏ vào cây cũ.
Cache chỉ được dùng khi metadata khớp; sửa/thay/xóa file không làm tiếp tục phục
vụ nội dung cache cũ. Thay nội dung giữ nguyên mtime vẫn đổi validator khi ctime
đổi. Cache chỉ chứa file đã đọc hoàn tất và metadata không đổi trong khi nạp.
HEAD, 304 và range miss không bắt buộc nạp toàn file vào cache.

Thiếu budget/headroom, cache đầy với entry đang được response giữ, hoặc cấp phát
cache thất bại: bỏ qua cache và tiếp tục stream disk. Không trả 503 chỉ vì cache
không đủ RAM. Cache là dữ liệu tạm; không được lưu bền vững qua restart.

## `load_to_ram=true`: bắt buộc preload đầy đủ

Flag này nạp sẵn toàn bộ cây file khi chuẩn bị config. Không có bước đọc nội dung
file từ disk trên đường phục vụ request sau đó. `cache_max_file_bytes` không giới
hạn file preload; mọi file đều phải nằm trong budget, không có preload một phần.

Prepare/validate thực hiện:

1. Duyệt manifest có giới hạn số entry, chiều sâu 64 và path 4096 byte. Entry
   gồm cả directory; symlink, special file hoặc tên không phải UTF-8 bị từ chối.
2. Cộng kích thước bằng checked arithmetic, kể cả file sparse và phần charge
   metadata/path. Kiểm tra `max_ram_bytes` và budget chung của proxy.
3. Kiểm tra `MemAvailable`, các giới hạn cgroup v2 nhìn thấy được và `RLIMIT_AS`,
   chừa `ram_headroom_bytes`. Không xác minh được thì từ chối preload.
4. Cấp phát bằng `try_reserve_exact`, đọc có trần theo manifest, kiểm tra lại
   metadata và cả cây thư mục. File/cây thay đổi trong lúc nạp làm candidate lỗi.
5. Chỉ commit sau khi toàn bộ route đã chuẩn bị thành công.

Vượt giới hạn byte/số entry, thiếu RAM/headroom, lỗi cấp phát hay đọc file đều
**fail closed**: startup thất bại, hoặc validate/apply trả lỗi và giữ nguyên
config/runtime hiện hành. Không tự chuyển route preload sang disk.

Budget chung tính cả cache disk, RAM image hiện hành, candidate và generation cũ.
Bytes còn được response giữ sau eviction cũng giữ reservation. Cần đủ chỗ cho
cả ảnh cũ và ảnh mới trong quá trình rolling; chỉ nhả budget khi hết tham chiếu.
Budget là dung lượng payload cộng charge metadata, không phải giới hạn RSS toàn
daemon. Kiểm tra RAM khả dụng là tại thời điểm prepare; không giữ chỗ bộ nhớ của
toàn host trước biến động từ tiến trình khác. Triển khai vẫn cần memory limit
và headroom phù hợp.

RAM preload là snapshot: sửa file nguồn không tự thay nội dung đang phục vụ.
Full `prostctl validate/apply/reload` đọc lại và kiểm tra ảnh mới; TLS-only reload
tái sử dụng ảnh hiện hành. Không có polling/watcher. Sau restart, file nguồn
phải còn truy cập được để preload lại.

## Quan sát và kiểm thử

`prostctl status` trả `static_files` theo listener/route: mode, số entry, byte
reservation, hit/miss; `static_files_ram` trả budget/reservation toàn proxy.
Prometheus có `prost_static_ram_reserved_bytes` và `prost_static_ram_budget_bytes`.

`prost check` chỉ kiểm tra schema. Dùng `prostctl validate --file candidate.toml`
để kiểm tra mở file và khả năng preload trên daemon thực sự.

```sh
sh scripts/docker-dev.sh cargo test -p prost-core --test proxy static_ --locked
sh scripts/check-static-memory.sh
```

Bài thứ hai chạy test binary đã build trong container **96 MiB**, dùng file sparse
96 MiB và headroom 16 MiB. Route/process budget vẫn đủ nhưng cgroup không đủ;
test yêu cầu từ chối trước allocation, giữ config và `oom_kill=0`. CI chạy riêng
bài này; nó được ignore trong suite không có memory limit tương ứng.

Cache file tĩnh ở đây khác cache response upstream trên đĩa; HTTP proxy disk cache
vẫn là hạng mục riêng.
