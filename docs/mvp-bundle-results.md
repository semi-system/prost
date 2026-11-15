# Mức sẵn sàng và kiểm chứng bản native mở rộng

Báo cáo này ghi image `25a01bb041ec` trước phần PROXY v2 egress/mail health/TLS
policy. Bản mail service mới hơn và bằng chứng tương ứng: [mail-interop-results.md](mail-interop-results.md).

Đánh giá hiện tại: **dùng cho MVP/pilot có giám sát**. Chưa xác nhận đủ điều kiện
cho production quan trọng. Các số đo dưới đây thuộc fixture local Docker/WSL,
không phải capacity cam kết trên máy production và không chứng minh ngang nginx.

Phạm vi chức năng và giới hạn được mô tả trong [mvp-bundle.md](mvp-bundle.md).
Những tùy chọn mới mặc định tắt; nên bật từng nhóm theo nhu cầu thực tế.

## Kiểm thử mã nguồn

Chạy bằng Docker Rust 1.93, dependency khóa bằng Cargo.lock, hai build job và
`RUST_TEST_THREADS=4`:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --all-features --locked
```

131 test đạt ở mỗi chế độ native và all-features. Có một helper crash-test được
đánh dấu ignored để test cha chủ động gọi trong subprocess, không phải ca chưa
chạy. Log: `test-results/bundle-final-checks.log`. Sau đó chạy bổ sung ca
`gzip_brotli_cache_privacy_retry_and_trailers` với write guard H2 bật: đạt.

Các ca mới kiểm tra:

- Route index giữ thứ tự priority/prefix/declaration so với linear scan.
- Gzip/Brotli giải nén đúng byte, giữ trailers; retry GET và không replay POST.
- Cache bỏ qua dữ liệu riêng tư, conditional/upgrade; only-if-cached không gọi
  origin; capture hủy/oversize trả budget; DATA bị giữ sau eviction vẫn giữ budget;
  thời gian nhận body không kéo dài freshness.
- H2 initial window 0 và window 1 giữ frame cuối: reset stream bị kẹt, trả slot
  endpoint, ghi aborted và các stream khác trên connection tiếp tục chạy.
- HEADERS/CONTINUATION chưa hoàn chỉnh sau request đầu bị deadline đóng connection.
- PROXY v1/v2 kiểm tra nguồn đáng tin và địa chỉ client dùng trong ACL.
- mTLS downstream/upstream, thiếu client identity bị từ chối; restart từ artifact
  đã stage sau khi xóa certificate/key nguồn; TLS passthrough giữ TLS đầu cuối.
- Pool idle/busy theo stream đang giữ/cancel; plugin counters và compact log filter.

Một lượt đầu chạy harness không giới hạn thread bị lỗi timing ở fixture deadline
50 ms có sẵn. Ca đó đạt 10 lần chạy riêng; toàn bộ suite đạt khi giới hạn bốn
thread. Giữ log đầu và log lặp tại `test-results/bundle-checks-first.log` và
`test-results/bundle-header-repeat.log`; không tính lượt đầu là đạt.

## Xác nhận image release

Image native release, Rust 1.93, thin LTO và WASM tắt:
`sha256:25a01bb041ecb86225abc718f7f88dbd611e49db952656c95f893775af38c642`.
Ba example advanced-native/mTLS/PROXY đều qua `prost check`. Demo HTTP/TCP,
headers/maps/rewrite/redirect, balancing/backup/capacity/passive health, validate,
atomic apply và giữ phiên cũ đều đạt.

Hai fault smoke trên image này đạt:

- 10.000 H2 request qua H1 upstream khi ép log tmpfs 64 KiB đầy thật: traffic
  tiếp tục, queue có giới hạn, writer báo lỗi và phục hồi qua reopen; rotate,
  generation conflict, hot disable và restart giữ cấu hình đúng. Working set
  cgroup đỉnh 10,00 MiB, FD đỉnh 99. Log bị mất khi đĩa đầy được đếm; đây không
  phải cam kết log không mất.
- 10.000 response streaming, tổng 2.621.440.000 byte, không lỗi; 10 lượt apply/GC
  khi đang tải, xóa 100 orphan artifact, restart bền vững và rollback khi ENOSPC.

Raw: `test-results/bundle-final-access-smoke/summary.json` và
`test-results/bundle-final-artifact-smoke/summary.json`. Benchmark cuối dùng tiền
tố `bundle-final-`, log runner dùng `final-bundle-`, tách khỏi các lượt thử đầu.

Control là image trước thay đổi: `prost:bundle-control`, ID
`sha256:41e2eb6cd03f6b1c0f9b75f99bef59e73a3f505bf9725a7f10d4f92a1bf3f0a7`.
Image thử đầu `prost:bundle-first` có ID
`sha256:1dd34eb3cd25597977a61ca9854e37516dc16fc042911f9570594de39f9c864d`.
Lượt log benchmark đầu bị dừng do trùng thời gian compile; không dùng kết quả
đó để kết luận regression. Tag `prost:native` có thể đổi, phải đối chiếu image ID
trong từng báo cáo JSON.

## Đối chứng trước phần mail service

CPU pin client/proxy/backend `0,2` / `4,6` / `8,10`; hai proxy worker,
control `41e2…` và candidate `25a0…`, cùng workload không cache. Không compile,
soak hoặc tải nginx đồng thời. Các bài riêng đổi cấu hình được ghi rõ.

| Bài | Round × giây/image | RPS trước → sau | p99 ms trước → sau | Gate |
|---|---|---|---|---|
| H2 TLS 64 KiB, mặc định | 5 × 12 | 9.734 → 9.872 | 23,532 → 27,213 | p99 +15,64%, cần điều tra |
| Native header 34 B, mặc định | 5 × 12 | 30.923 → 29.993 | 12,382 → 13,251 | Trong ngưỡng |
| Native header 34 B, full JSON log | 3 × 8 | 25.625 → 25.620 | 13,314 → 13,303 | Trong ngưỡng |
| 34 B, cùng candidate: off → compact sample1/16 + reuse | 3 × 8 | 22.109 → 27.098 | 15,949 → 13,779 | Trong ngưỡng; RPS tăng không chứng minh log làm nhanh hơn |
| H2 64 KiB, cùng candidate: off → compact sample1/16 + reuse | 3 × 8 | 10.854 → 8.034 | 23,848 → 32,301 | RPS -25,98%, p99 +35,45%, cần điều tra |

Không có failure trong các lượt. Request đang dở khi timed interval kết thúc
được báo riêng trong raw JSON, không tính thành response thành công. Gate là
throughput giảm trên 5% hoặc p99 tăng trên 10%. Host dao động rõ giữa round;
không kết luận từ một số median rằng overhead đã biến mất. H2/logging vẫn còn
vượt gate; không đánh dấu mục tiêu tối ưu hiệu năng là hoàn tất.

Raw: `test-results/bundle-final-default-bench`, `bundle-final-full-log-bench`,
`bundle-final-compact-bench`, mỗi thư mục có summary và các round/image/config ID.

## Soak và triển khai

Lượt thử 60 giây trên image release cuối, gồm H2 header/write guard:
32.072 H1 request, 1.100 probe H2/TLS, 11 hot apply, không có lỗi traffic;
RSS đỉnh 21,00 MiB, FD đỉnh 121. Hai generation khi phiên TCP giữ cấu hình cũ;
sau đóng traffic còn một generation và không còn connection. Có cache hit,
rotate log và GC trong lúc tải. Đây chỉ là smoke, **không phải soak 24 giờ**.
Raw: `test-results/bundle-final-soak-smoke/summary.json`.

Soak đủ 24 giờ chưa được xác nhận đạt. Khi chạy, theo dõi
`test-results/soak-24h-edge/progress.json` (image 89da…, trước Runtime API rate
table; lượt v2 đã dừng có chủ đích). Chỉ `state=passed` cùng `summary.json`
sau đủ thời gian mới là kết quả hoàn tất. WSL/Docker phải tiếp tục chạy suốt lượt.

Để đưa vào pilot, giữ image ID đã test, lưu cấu hình/artifact trên volume bền vững,
giới hạn CPU/RAM/connection và theo dõi lỗi, p99, RSS, FD, live generation.
Chuẩn bị đường chuyển traffic về proxy đang vận hành nếu pilot gặp lỗi.

Trước production quan trọng còn cần kết quả soak đủ thời gian, giải thích các
regression vượt gate trên phần cứng đại diện, kiểm chứng capacity theo workload
thật, và kiểm tra vận hành/khôi phục/quyền admin socket-TLS theo môi trường triển
khai. Qua kiểm thử chức năng không tự động đáp ứng các điều kiện này.
