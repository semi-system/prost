# Tối ưu Prost — 2026-11-07

Kết quả mới hơn với PGO và nginx được tăng pool cho tương đương nằm ở
[pgo-performance.md](pgo-performance.md). Các bảng dưới giữ lại từng giai đoạn
để phân biệt thay đổi runtime với thay đổi compiler.

Đợt này tối ưu đường xử lý hiện có, không thay Tokio/Hyper bằng TCP/IP stack
mới và không bỏ các ràng buộc timeout, generation hay admission để lấy req/s.

## Thay đổi đã triển khai

- Không tạo plugin event, completion channel hoặc end-hook task nếu request
  không có plugin cần hook. Native connection limit vẫn giữ permit suốt phiên.
- Body cụ thể dùng pin projection an toàn thay hai lớp boxed body. Idle timer
  nằm inline và chỉ khởi tạo khi đọc body thực sự phải chờ.
- Với response HTTP/1 có độ dài nhỏ đã biết, chỉ poll thử một frame không chờ.
  Nếu body đã đủ, chuyển `Bytes` trực tiếp để thuận lợi ghi chung header/body.
  Nếu còn dữ liệu, giữ frame đầu và tiếp tục stream; không đợi gom cả body.
- Scheme/authority upstream được parse khi chuẩn bị config; request tái sử dụng
  URI parts thay vì format và parse URL lại. Forwarded metadata tạo một lần
  mỗi connection; tên header thường dùng được chuẩn bị sẵn.
- Route được sắp thứ tự trước, giữ trực tiếp upstream đã chuẩn bị. Trường hợp
  không route đi thẳng tới default upstream; endpoint đơn không chạy hai vòng
  tính tổng weight và tăng counter round-robin.
- Pool HTTP mặc định từ 16 lên **128 idle connection/host**, timeout 30 giây,
  có thể đổi qua `pool_max_idle_per_host` và `pool_idle_timeout_ms`. Đây là chi
  phí RAM/FD có chủ đích để giảm tạo lại TCP/DNS; không phải response cache.
- Giữ lại plugin state/permit khi upstream thay đổi nhưng plugin không đổi.
  Body và WebSocket tunnel giữ cả generation lẫn admission của connection.
- Relay TCP dùng timestamp atomic thay mutex hoạt động; shutdown bị treo cũng
  có deadline. Chưa đo riêng throughput TCP bulk cho thay đổi này.
- Binary CLI giới hạn blocking pool 32 thread. Dockerfile mặc định build
  release; profile release vẫn dùng LTO thin và một codegen unit như baseline.

HTTP request mới vẫn lấy snapshot mới trên keep-alive/HTTP2. TCP, WebSocket,
gRPC và response đang phục vụ giữ tài nguyên generation của mình. Khi compile
upstream binding, listener không được tái sử dụng nhầm reference upstream cũ.

## Những thử nghiệm không giữ lại

Đã thử chia client pool theo worker; chưa có lợi ích đủ rõ ở các lượt đo, nên
không giữ thay đổi đó hay thêm schema `pool_shards` vào bản phát hành.

Chưa thay allocator hoặc tự viết HTTP parser. CPU profile phẳng bằng `perf`
cho thấy chi phí rải trên HTTP stack, cấp phát và kernel I/O, không có một hàm
user-space đơn lẻ áp đảo. Profile giai đoạn đầu có khoảng 3.200 sample và không
mất sample; đây là diagnostic 100.000 request, không dùng làm bảng throughput.
Các file ở `test-results/profile-stage1/` và `test-results/profile-nginx/`.

## Kiểm thử

**22 test pass:** 20 integration test và 2 unit test HTTP. Có kiểm tra bổ sung:

- Route ưu tiên/host/path đúng và upstream binding đổi ngay trên keep-alive.
- Body nhỏ hoặc chia nhiều frame không mất byte, chạy với 32 client đồng thời.
- Body chậm timeout và giải phóng request permit.
- End hook chạy sau body; route-only plugin không bị bỏ qua.
- Connection limit không reset khi apply upstream; WebSocket giữ admission đến
  khi tunnel đóng.
- Các ca cũ: TLS hai phía/rotation, HTTP2/gRPC trailers, config conflict,
  restart persistence, WASM update, stream giữ phiên và graceful shutdown.
- Runtime I/O hai worker, flush histogram đủ 320/320 mẫu khi shutdown và chế
  độ shared-runtime fallback vẫn phục vụ HTTP, ghi metrics chính xác.

Clippy với `-D warnings` đã pass. Build Docker release cũng đã chạy thành công.

## Cách đo và phạm vi kết luận

Đo bản baseline release đã lưu, bản release mới và nginx trong cùng lượt.
Mỗi proxy có 2 CPU quota/256 MiB, cùng static backend 34 byte. Mỗi vòng dùng
1.000 request warmup rồi 10.000 request đo, concurrency 100, ba vòng đổi thứ tự.
Driver là ApacheBench HTTP/1.0 keep-alive; không TLS/WASM/cache. Không build
hay profile đồng thời với lượt đo cuối.

nginx giữ nguyên config 16 idle connection/worker × 2 worker; Prost mới có 128
idle connection/host dùng chung. Cải thiện bao gồm cả thay code và thay default
pool, không phải bằng chứng Rust nhanh hơn C. RAM là cgroup working set lấy mẫu
50 ms, không phải heap hoặc RSS duy nhất của ứng dụng.

Raw kết quả cuối: `test-results/optimized-final/`. Baseline image được giữ ở
`prost:baseline-release`; image được đo mới là chính image release `prost:mvp`.

## Giai đoạn 1: kết quả 10.000 request, runtime chia sẻ

Mỗi bản hoàn tất **30.000/30.000 request**, không lỗi. Bảng dùng trung vị
throughput/p99/CPU của ba lượt; RAM là đỉnh lấy mẫu lớn nhất của ba lượt.

| Chỉ số | Prost baseline release | Prost mới release | nginx 1.28.0 |
|---|---:|---:|---:|
| Throughput | 15.267 req/s | 31.565 req/s | 39.986 req/s |
| p99 | 28 ms | 5 ms | 5 ms |
| CPU/request | 132,0 µs | 62,7 µs | 49,0 µs |
| RAM working set đỉnh | 17,22 MiB | 13,86 MiB | 7,54 MiB |

Trong bài đo này, bản mới đạt **2,07× throughput**, giảm khoảng **52,5% CPU/request**
và **19,5% RAM đỉnh lấy mẫu** so với baseline release. Còn khoảng 21% thấp hơn
nginx về throughput và 28% cao hơn về CPU/request. Chưa ngang nginx về RAM.

Throughput từng lượt: baseline 15.267 / 11.847 / 17.080; mới 28.735 / 31.565 /
32.180; nginx 41.819 / 39.986 / 39.490 req/s. Máy dùng chung và các cửa sổ đo
ngắn có dao động; những số phần trăm trên không phải bảo đảm cho mọi workload.

**Không bật response cache ở nginx.** Config chỉ có `proxy_pass`, upstream
keep-alive và tắt buffering. Backend dùng `return 200` để tạo cùng payload
cho mọi proxy, không phải proxy trả kết quả từ cache.

## Giai đoạn 2: I/O worker riêng

Đã triển khai listener `SO_REUSEPORT` riêng, runtime current-thread riêng và
HTTP pool gắn với từng worker. Các socket/driver được chuẩn bị trước commit
config. Semaphore toàn cục và snapshot generation vẫn giữ đúng semantics.
Root runtime phục vụ control/health; shutdown drain xong mới dừng I/O workers.

Trong `test-results/worker-local-100k/`, mỗi bản chạy ba lượt × 100.000 request:

| Chỉ số | Prost worker-local | nginx |
|---|---:|---:|
| Throughput trung vị | 41.218 req/s | 41.609 req/s |
| CPU/request | 48,25 µs | 47,55 µs |
| p99 trung vị | 4 ms | 5 ms |
| RAM đỉnh lấy mẫu | 14,14 MiB | 8,20 MiB |
| Lỗi / request | 0 / 300.000 | 0 / 300.000 |

Kết quả gần ngang, **chưa chứng minh vượt nginx**. Pure pool sharding với
runtime multithread ở thử nghiệm trước khác với chế độ này: ở đây cả I/O và
driver connection giữ trên worker của nó, không chỉ chia cache.

Đã bổ sung histogram local, flush mỗi 64 mẫu hoặc 250 ms; counters không bị bỏ.
Trong `test-results/local-metrics-100k/`, Prost đạt 41.590 req/s, nginx 42.499;
vẫn gần ngang trong độ dao động. WASM engine nay chỉ khởi tạo khi có plugin
WASM; các plugin vẫn được compile/validate trước publish config.

API Rust: truy cập engine qua `PluginRegistry::engine() -> Result<&Engine>`;
không còn field eager engine. `runtime.worker_local` mặc định true,
`runtime.io_workers=0` auto tối đa 8; thay hai giá trị cần restart. Có fallback
shared runtime qua `worker_local=false`.

Đợt đo reuse sau bước 1 ghi nhận Prost mở **0 TCP mới** ở cả ba lượt sau warmup,
vẫn forward đủ 10.000 request tới backend mỗi lượt. Raw:
`test-results/optimized-upstream-reuse.json`. Việc giảm connection churn không
phải caching response.

Các lượt thử `optimized-stage1`, `optimized-stage2`, `optimized-candidate` chỉ
phục vụ lựa chọn phương án; bảng kết luận dùng lượt cuối. Riêng stage1 có một
lần Clippy gần thời gian đo nên không dùng làm kết quả chính thức.

Không suy rộng kết quả sang TLS, payload lớn, nhiều route hoặc plugin WASM.
Benchmark dài và phần cứng load generator riêng vẫn cần cho production sizing.
