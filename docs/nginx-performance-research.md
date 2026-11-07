# nginx tối ưu ở đâu, và Prost đang trả thêm chi phí gì?

Đối chiếu nginx **release-1.28.0**, commit
`481d28cb4e04c8096b9b6134856891dc52ecc68f`, với code và dependency trong
`Cargo.lock` của Prost. Nghiên cứu ngày 2026-11-07, chưa thay thuật toán proxy.

Bài đo trước cho thấy nginx dùng khoảng **46,8 µs CPU/request**, Prost release
**135,0 µs**, trên workload nhỏ. Đó là khoảng cách đã đo; những nguyên nhân dưới
đây có mức bằng chứng khác nhau. Chưa có flamegraph để phân chia phần trăm CPU.

## 1. Worker giữ phần lớn trạng thái xử lý cục bộ

Worker nginx chạy vòng xử lý I/O, timer và sự kiện chờ. Connection thông thường
được worker đó xử lý qua các trạng thái đọc, gửi upstream, nhận response và ghi
về client. Đường đi này không cần một scheduler task đa luồng cho từng bước.
Nguồn: [`ngx_process_events_and_timers`](https://github.com/nginx/nginx/blob/release-1.28.0/src/event/ngx_event.c#L195).

Prost cũng dùng I/O bất đồng bộ. Tokio có I/O driver, timer và scheduler;
runtime đa luồng có cơ chế điều phối/work-stealing. Đây là công cụ tổng quát,
không có nghĩa mọi `.await` đều tạo thread hay mọi request đều đổi core.
Nguồn: [Tokio runtime](https://docs.rs/tokio/1.54.1/tokio/runtime/index.html).

Khác biệt cụ thể trong Prost: một `HttpClient` được chia sẻ theo upstream giữa
các task. Source dependency hyper-util 0.1.21 tại
`src/client/legacy/pool.rs` chứa `Arc<Mutex<PoolInner<...>>>`. nginx dùng cache
connection trong từng worker. Điều này tạo khả năng giảm đồng bộ và tăng tính
cục bộ bộ nhớ cho nginx; chi phí contention thực tế ở Prost vẫn cần profile.

## 2. Cấp phát theo vòng đời request và tái sử dụng buffer

Trong `ngx_palloc_small`, nginx lấy vùng trống trong block của pool rồi tăng
con trỏ; khi hết chỗ mới cấp block khác. Pool gắn với vòng đời đối tượng giúp
giảm các lần cấp phát nhỏ độc lập. Đây không phải cơ chế zero-copy cho toàn bộ
network traffic. Nguồn: [`ngx_palloc.c`](https://github.com/nginx/nginx/blob/release-1.28.0/src/core/ngx_palloc.c#L149).

Trong Prost, một request đi qua nhiều đối tượng riêng: chuỗi host/peer/upstream,
URI được format rồi parse, `Vec` giữ permit, channel hoàn tất, body được box,
timer được box và task cho hook kết thúc. Có thể xác nhận những đối tượng này
từ source, nhưng chưa đếm số lần malloc hay số byte cấp phát/request.
`BTreeMap::new()` và `Vec::new()` rỗng không tự động đồng nghĩa đã cấp phát heap.

Vì thế ưu tiên bỏ đối tượng không cần trước; chưa có cơ sở để thay allocator
hoặc đưa một arena tùy biến vào toàn bộ HTTP stack.

## 3. Upstream keep-alive: đã có phép đo bổ sung

nginx dùng queue cache trong từng worker, lấy lại connection phù hợp và đưa
connection còn hợp lệ về cache. Cache item được tạo từ khi khởi tạo cấu hình.
Nguồn: [`ngx_http_upstream_keepalive_module.c`](https://github.com/nginx/nginx/blob/release-1.28.0/src/http/modules/ngx_http_upstream_keepalive_module.c).

Ở benchmark đã dùng:

- nginx `keepalive 16` × 2 worker: tối đa **32 connection idle** trong hai cache.
- Prost `pool_max_idle_per_host(16)`: **16 idle connection** trong client dùng chung.
- Đây là giới hạn idle, không phải giới hạn active connection. Cấu hình chưa
  ngang nhau hoàn toàn về dung lượng pool, nên không quy mọi chênh lệch cho kiến trúc.

Đã thêm `stub_status` chỉ vào backend benchmark và chạy
`scripts/probe-upstream-reuse.py`. Mỗi lượt warmup 1.000 request, sau đó 10.000
request với `ab -k -c 100`, cùng CPU/RAM như bài so sánh. Lấy chênh số TCP accept
ở backend trước/sau lượt tải; connection đọc status được giữ nguyên suốt lượt.

| Proxy | TCP mới lượt 1 | Lượt 2 | Lượt 3 |
|---|---:|---:|---:|
| Prost release | 1.643 | 2.615 | 1.648 |
| nginx | 363 | 321 | 335 |

Mỗi lượt có 10.000 request hoàn tất, không lỗi. Backend ghi nhận tăng 10.001
HTTP request, gồm một lần đọc status sau tải. Health probe TCP của Prost có thể
thêm connection trong cửa sổ đo; phép đo chưa tách riêng chúng.

**Bằng chứng:** cấu hình Prost hiện tái tạo TCP connection nhiều hơn rõ rệt.
**Chưa chứng minh:** tăng pool sẽ xóa bao nhiêu khoảng cách CPU/throughput.
Trung vị số TCP mới chênh khoảng 4,9×; cần thử các mức pool ngang nhau để phân
biệt dung lượng cache với cơ chế điều phối và trả connection.

Ngoài TCP handshake, endpoint `backend:8080` là hostname. Source hyper-util
0.1.21 ở `src/client/legacy/connect/dns.rs` dùng `GaiResolver` với
`spawn_blocking`/`to_socket_addrs`. Tái tạo connection có thể kéo thêm resolve và
blocking-worker work. Chưa đếm số DNS call thực tế; không suy số DNS query trực
tiếp từ số TCP accept, vì resolver hệ điều hành có thể cache.

## 4. Prost chưa có đường xử lý gọn cho trường hợp không plugin

Trong `crates/core/src/lib.rs`, `handle_http` vẫn:

- Tạo `Event`, chuỗi hook/peer và chuyển qua các bước plugin khi danh sách rỗng.
- Tạo một task chờ completion rồi chạy hook `end`, dù không có hook nào cần chạy.
- Giữ thêm tham chiếu snapshot/listener/proxy cho task đó.

Trong `crates/http/src/lib.rs`, `forward` luôn tạo oneshot completion; response
body được bọc hai lớp box và có một `Sleep` được box. Response headers được
sanitize ở cả lớp forward và lớp gọi; lần sau vẫn cần nếu plugin có thể sửa
header, nhưng đường không plugin có thể tránh làm lại.

Đây là phần việc có thể loại bỏ trong trường hợp không có plugin. `invoke` với
plugin list rỗng không serialize JSON hay copy toàn bộ headers: vòng lặp không
chạy. Không nên quy overhead WASM cho benchmark vốn không bật WASM.

## 5. I/O theo buffer chain

nginx có đường chuyển tiếp không buffering dùng lại các chain/buffer, dừng khi
I/O chưa sẵn sàng; writer có thể gom vùng nhớ thành iovec rồi gọi `writev`.
Nguồn: [upstream non-buffered path](https://github.com/nginx/nginx/blob/release-1.28.0/src/http/ngx_http_upstream.c#L3729),
[`ngx_writev_chain.c`](https://github.com/nginx/nginx/blob/release-1.28.0/src/os/unix/ngx_writev_chain.c).

Hyper cũng có hỗ trợ vectored I/O. Chưa có syscall trace để khẳng định Prost
gọi write nhiều hơn hoặc copy body nhiều hơn nginx. Chỉ đọc source không đủ
để định lượng lợi ích batching trong workload này.

## Những giải thích không phù hợp với bài đo hiện tại

- **Kernel bypass:** nginx dùng socket/kernel TCP; Linux có cơ chế epoll.
  Prost cũng dựa trên socket. [nginx event methods](https://nginx.org/en/docs/events.html).
- **Cache response/sendfile:** cache đã tắt; proxy chuyển response từ socket
  upstream, không phục vụ file local. Không có bằng chứng đây là lợi thế đã đo.
- **TLS hoặc WASM:** cả hai đều không bật trong benchmark.
- **Parse TOML/fsync config:** nằm trên đường cập nhật cấu hình, không chạy cho
  từng request. Hot reload không tự nó giải thích khoảng cách hiện tại.
- **C luôn nhanh hơn Rust:** đây là hai implementation khác nhau về pooling,
  scheduling và số công việc; benchmark chưa tách được ảnh hưởng ngôn ngữ.

## Thứ tự thử nghiệm nên làm tiếp

1. Đo pool ở các mức idle ngang nhau; ghi TCP accept/10k request, CPU/request,
   tail latency và memory. Không chỉ tăng pool rồi nhìn req/s.
2. Bỏ `Event`/completion channel/end task khi không có plugin cần hook; giữ
   nguyên semantics snapshot mỗi HTTP request và thời gian sống response body.
3. Profile release: allocator, scheduler wakeups, pool locks, DNS và syscall.
   Đo thêm cửa sổ dài hơn để giảm nhiễu quota/scheduler.
4. Dựa trên profile mới quyết định giảm boxing, prebuild URI authority, cache
   DNS có refresh, shard metrics/pool, hoặc thử runtime/worker cục bộ.

Chưa cần viết lại HTTP parser hay chuyển sang DPDK. Mỗi thay đổi phải vượt lại
test keep-alive nhận config mới, stream pin generation cũ, WebSocket, gRPC
trailers, cancellation, timeout và graceful shutdown.

Raw số đo bổ sung: `test-results/comparison/upstream-reuse.json`. Config probe
và script đã lưu trong repo; benchmark containers đã dừng sau kiểm tra.
