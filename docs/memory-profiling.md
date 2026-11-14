# Đo heap, RSS và bộ nhớ giữ lại

`scripts/profile-memory.py` chạy trong WSL bằng Docker, cần root cho procfs/cgroup
và `nsenter`. Cần `h2load`, `curl`, fixture/certificate đã chuẩn bị và image backend:

```sh
sudo python3 scripts/prepare-advanced-bench.py
sudo docker compose -f benchmarks/advanced/compose.yaml build backend
sudo python3 scripts/profile-memory.py --image prost:native \
  --output-dir test-results/my-memory
```

Harness dùng project/network/container riêng `prost-memory-profile`, port host
19881/19844, proxy 2 CPU/512 MiB, backend 2 CPU/256 MiB, hai control threads và hai
I/O workers. Backend không bật cache. Không chạy harness/benchmark khác đồng thời.
Những container không thuộc harness được giữ nguyên và liệt kê trong metadata.

Các stage cố định: startup, 10.000 request HTTP/1 34 byte và 1.000 HTTPS/1,
128 connection HTTP idle sau response, 32 TLS readers giữ body 16 MiB và mới đọc
4 KiB, 10 apply weight khi stream còn sống, cancellation và pool expiry. Client
không gom body lớn vào RAM. Idle pool timeout 1 giây để kiểm tra thu hồi trong
test ngắn; đây không phải đổi mặc định 30 giây của production. Cuối mỗi run phải
còn một generation và không còn accepted client connection.

Mỗi stage lưu `smaps_rollup`, FD/thread, cgroup, Runtime API status và exposition
Prometheus. Scrape dùng network namespace của container, curl nằm ngoài cgroup
proxy. RSS/Anonymous/PSS, cgroup working set và heap live có ý nghĩa khác nhau;
file pages có thể được charge cho container chạm trang đầu tiên. Không cộng các
số này thành một tổng RAM. RSS còn có code, stack và allocator retention; virtual
memory không phải RAM resident. Stages là mẫu tại thời điểm đó, có thể bỏ lỡ spike.

## Đối chứng nhiều vòng

Giữ image trước khi thay code, không gắn tag baseline cho binary mới:

```sh
sudo docker tag prost:native prost:metrics-control  # làm trước khi build thay đổi
# Build image mới bằng Dockerfile hoặc build-release-benchmark.py.
sudo python3 scripts/compare-memory.py --before prost:metrics-control --after prost:native \
  --rounds 3 --include-buffer-control --output-dir test-results/memory-final
```

Thứ tự ba điều kiện luân phiên: binary trước, binary mới mặc định 64 KiB, binary
mới với `buffer_limit_bytes=16384`. Tất cả có process/state mới, traffic và scrape
giống nhau, không override allocator. Summary ghi trung vị, raw RSS từng vòng,
anonymous/cgroup, FD và generation. Không suy ra RPS từ thời gian harness này.

`--env NAME=VALUE` cho phép thí nghiệm allocator deployment, ví dụ
`--env MALLOC_ARENA_MAX=2`, nhưng không tự thay global allocator hay environment
của production. Cần kiểm tra throughput/CPU riêng trước khi chọn tuning đó.

## Heaptrack tách riêng khỏi benchmark tốc độ

```sh
sudo docker build -f benchmarks/profiling/Dockerfile \
  --build-arg PROST_IMAGE=prost:native -t prost:heap-after .
sudo python3 scripts/profile-memory.py --image prost:heap-after --heap \
  --tls-buffer-limit 16384 --output-dir test-results/memory-heap-final
```

Image profiler cài heaptrack/procps, không đưa chúng vào image production.
Heaptrack được preload khi launch process, không attach bằng ptrace. Sau traffic
và cancellation, gửi SIGTERM cho Prost, chờ graceful exit để flush trace; đọc bằng
`heaptrack_print` trong container. Output gồm trace nén, allocation report và
massif có số byte heap theo thời gian. Binary release giữ function symbols nên
có thể quy allocation về Hyper/rustls/Tokio; không có line-level debug info đầy đủ.

Profiler làm tăng RSS, CPU và thời gian chạy. Chỉ dùng trace để phân bổ heap và
tìm allocation hotspots; dùng các run không profiler để kết luận RSS/tốc độ.
Đỉnh từng call stack có thể xảy ra khác thời điểm, không cộng các đỉnh đã merge
thành peak heap tổng. Những allocation còn lại lúc exit cũng chưa đủ để kết luận
memory leak; có thể là state global của thư viện/process. Xem
[heaptrack](https://github.com/KDE/heaptrack) và raw massif.

Đối chứng tốc độ riêng:

```sh
sudo python3 scripts/compare-header-images.py --before prost:metrics-control --after prost:native \
  --pin-cpus --rounds 5 --output-dir test-results/metrics-header-final
sudo python3 scripts/compare-header-images.py --before prost:metrics-control --after prost:native \
  --pin-cpus --rounds 3 --scale 1 --cases tls_h1_64k h2_64k double_tls_h2_64k h2_stream_256k grpc_stream \
  --output-dir test-results/metrics-heavy-final
```

Giữ cả những run vượt regression gate. Buffer thấp có thể giảm heap nhưng tăng
fragmentation record/write và chi phí CPU; kết quả chỉ bao phủ workload đã đo.

## Đối chứng theo thời gian và thông tin worker

`compare-header-images.py --duration 10` chạy timing-based h2load, giữ clients
hoạt động tới deadline thay vì để từng connection hết quota request sớm:

```sh
sudo python3 scripts/compare-header-images.py --before prost:metrics-control --after prost:native \
  --pin-cpus --rounds 5 --duration 10 --cases native_full_34 h2_64k \
  --output-dir test-results/metrics-timed-control
```

Đây là phương pháp đo khác fixed request. Khi SO_REUSEPORT phân phối các TCP
connection không đều, kết thúc theo quota request trên client có thể có phần đuôi
chỉ một worker còn tải. CPU/request gần nhau nhưng wall throughput lệch nhiều là
dấu hiệu cần điều tra điều này, không tự chứng minh code không có regression.
Harness ghi CPU ticks của từng thread trước/sau để kiểm tra mức sử dụng worker.

Ở mode timed, chỉ các request hoàn tất có status/latency samples tham gia mẫu.
`unfinished_at_deadline` ghi riêng những request đã bắt đầu nhưng bị dừng khi hết
giờ; không gọi chúng là proxy errors. Byte response có thể gồm phần body chưa
hoàn tất, nên kiểm tra nó nằm giữa completed×size và started×size, cùng preflight
byte integrity. Fixed-request mode vẫn yêu cầu đủ mọi request và đúng byte tuyệt
đối. Cả hai mode phải giữ kết quả riêng; timed không xóa bằng chứng negative của
fixed-request control.

Xuất báo cáo từ raw đã lưu bằng `python3 scripts/report-observability.py`;
[kết quả đo](observability-memory-benchmark.md) giữ cả các ca vượt gate và lựa
chọn cuối cùng về buffer mặc định.
