# Đo streaming limits và granular timeouts

Chạy Docker trong WSL, release Rust 1.93/thin LTO; WASM tắt. Khi đo throughput, proxy và backend mỗi bên 2 CPU/256 MiB. Khi profile RSS, proxy dùng 2 CPU/512 MiB và backend 2 CPU/256 MiB. Client/proxy/backend pin vào các guest core riêng ở lượt throughput; chỉ một proxy chạy mỗi lần, đảo thứ tự ảnh và workload theo round. Cùng backend/config/pool, không bật cache. Đây là phép đo trong máy phát triển, không phải cam kết production hay so sánh nginx mới.

Gate: throughput giảm trên 5% hoặc p99 tăng trên 10% cần điều tra. CPU lấy từ cgroup chia request hoàn tất; WS là working set cgroup, không phải RSS riêng tiến trình. Không build hay chạy profiler đồng thời với throughput test.

Fixed request có thể chịu ảnh hưởng đuôi connection và phân phối reuseport. Timed run giữ workload trong cửa sổ ghi ở từng phần, ghi riêng stream còn dở ở deadline. Các phương pháp đều được giữ lại; kết quả mới không xóa kết quả cũ.

Bản hiện tại giữ TCP upstream trong enum nhỏ, đưa TLS/timer ghi tùy chọn ra allocation riêng theo connection, và tách future của nhánh policy khỏi future chuyển tiếp mặc định. Header timer nằm inline trong future policy; khi headers deadline tắt và không theo dõi upload thì gửi thẳng vào client pool. Các phép đo dưới đây so bản timeout đầu tiên với bản tối ưu, không suy ra lợi ích từ việc cộng phần trăm của các phiên khác nhau.

## Bản tối ưu hiện tại — policy mới chưa bật

Raw: `test-results/http-limits-compact-default/`; 5 rounds, 8 giây/lượt.

| Workload | RPS trước → sau | Δ RPS | p99 ms trước → sau | CPU µs/request trước → sau | Peak cgroup WS MiB trước → sau | Gate |
|---|---:|---:|---:|---:|---:|---|
| native_full_34 | 36681 → 38284 | +4.37% | 8.526 → 7.615 | 54.14 → 52.01 | 47.50 → 47.19 | pass |
| h2_64k | 13001 → 12849 | -1.17% | 16.119 → 15.411 | 153.66 → 155.35 | 61.15 → 57.95 | pass |

Hoàn tất 1,977,240 request trước / 2,024,028 request sau; lỗi đã ghi nhận: 0. Stream chưa hoàn tất tại deadline: 2,560 (tách riêng, không tính thành request thành công).

Image trước: `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`; sau: `sha256:fe8dd2a6d10342b07861a306d7cf275fd1a0767374340564185ecb1f5a74c29b`.

Config SHA256 trước/sau: `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0` / `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0`.

## Bản tối ưu hiện tại — cùng policy được bật ở cả hai bản

Raw: `test-results/http-limits-compact-policy/`; 3 rounds, 6 giây/lượt.

| Workload | RPS trước → sau | Δ RPS | p99 ms trước → sau | CPU µs/request trước → sau | Peak cgroup WS MiB trước → sau | Gate |
|---|---:|---:|---:|---:|---:|---|
| native_full_34 | 32861 → 34837 | +6.01% | 10.637 → 9.419 | 59.31 → 57.00 | 59.43 → 59.54 | pass |
| h2_64k | 11540 → 12349 | +7.01% | 19.453 → 16.632 | 173.07 → 161.77 | 64.16 → 64.10 | pass |
| tls_h1_64k | 12745 → 12400 | -2.70% | 15.419 → 17.266 | 154.30 → 159.67 | 58.89 → 57.11 | CHECK |
| double_tls_h2_64k | 10149 → 9711 | -4.32% | 23.552 → 25.206 | 197.09 → 205.03 | 62.67 → 60.29 | pass |
| h2_upload_1m | 145 → 142 | -2.29% | 461.036 → 454.977 | 4651.90 → 4699.37 | 60.89 → 57.27 | pass |
| grpc_stream | 1784 → 1720 | -3.58% | 52.265 → 52.949 | 1033.73 → 1025.77 | 62.02 → 57.74 | pass |

Hoàn tất 1,267,820 request trước / 1,259,216 request sau; lỗi đã ghi nhận: 0. Stream chưa hoàn tất tại deadline: 3,672 (tách riêng, không tính thành request thành công).

Vượt gate ở `tls_h1_64k`. Giữ kết quả này để điều tra; không coi là đã đạt toàn bộ gate.

Image trước: `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`; sau: `sha256:fe8dd2a6d10342b07861a306d7cf275fd1a0767374340564185ecb1f5a74c29b`.

Config SHA256 trước/sau: `026bf9174b23e7df18844b715dec681f9fe58efdcb1ea904da5cb530fadf5ac8` / `026bf9174b23e7df18844b715dec681f9fe58efdcb1ea904da5cb530fadf5ac8`.

## Kiểm tra riêng TLS H1 sau gate p99 ở lượt hỗn hợp

Raw: `test-results/http-limits-compact-tls-focused/`; 5 rounds, 10 giây/lượt.

| Workload | RPS trước → sau | Δ RPS | p99 ms trước → sau | CPU µs/request trước → sau | Peak cgroup WS MiB trước → sau | Gate |
|---|---:|---:|---:|---:|---:|---|
| tls_h1_64k | 11914 → 12530 | +5.17% | 21.387 → 20.343 | 161.25 → 154.84 | 35.88 → 35.19 | pass |

Hoàn tất 609,680 request trước / 623,906 request sau; lỗi đã ghi nhận: 0. Stream chưa hoàn tất tại deadline: 1,000 (tách riêng, không tính thành request thành công).

Image trước: `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`; sau: `sha256:fe8dd2a6d10342b07861a306d7cf275fd1a0767374340564185ecb1f5a74c29b`.

Config SHA256 trước/sau: `026bf9174b23e7df18844b715dec681f9fe58efdcb1ea904da5cb530fadf5ac8` / `026bf9174b23e7df18844b715dec681f9fe58efdcb1ea904da5cb530fadf5ac8`.

Lượt TLS riêng giữ đúng hai image/config như lượt hỗn hợp, tăng lên 5 round × 10 giây và chỉ chạy workload TLS H1. Kết quả riêng không xóa hàng CHECK trong workload hỗn hợp; hai cách đo giúp đánh giá độ ổn định của regression.

## RSS của bản tối ưu

Ba round, mỗi profile 10.000 HTTP + 1.000 TLS request; 128 idle clients, 32 slow TLS streams, 10 apply rồi cancel/pool expiry. Không dùng heaptrack.

| Stage | Bản timeout đầu MiB | Tối ưu mặc định MiB | Tối ưu bật policy MiB |
|---|---:|---:|---:|
| startup | 9.03 | 8.98 | 9.09 |
| after_load | 20.32 | 19.86 | 19.62 |
| idle_clients | 20.56 | 20.09 | 19.85 |
| slow_tls_streams | 27.83 | 27.90 | 28.03 |
| after_10_updates | 28.05 | 28.07 | 28.11 |
| after_cancel | 25.27 | 27.09 | 23.30 |
| after_pool_expiry | 25.27 | 27.09 | 23.30 |

`before`: FD cuối 24, generations cuối [1, 1, 1]; image `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`.

`after`: FD cuối 24, generations cuối [1, 1, 1]; image `sha256:fe8dd2a6d10342b07861a306d7cf275fd1a0767374340564185ecb1f5a74c29b`.

`after_limits`: FD cuối 24, generations cuối [1, 1, 1]; image `sha256:fe8dd2a6d10342b07861a306d7cf275fd1a0767374340564185ecb1f5a74c29b`.

RSS sau cancel của bản mặc định có stage cao hơn bản trước; không cam kết RAM giảm ở mọi thời điểm. Allocator có thể giữ trang sau hủy. Harness kiểm tra 0 connection và 1 generation ở cuối từng profile; dữ liệu này chưa thay thế soak test dài hạn.

Các phép đo dưới đây được giữ để kiểm tra biến động và lịch sử điều tra, gắn với image ID của từng lượt.

## Đối chứng lại trước tối ưu — chỉ header nhỏ và H2 64 KiB

Raw: `test-results/http-limits-focused/`; 5 rounds, 10 giây/lượt.

| Workload | RPS trước → sau | Δ RPS | p99 ms trước → sau | CPU µs/request trước → sau | Peak cgroup WS MiB trước → sau | Gate |
|---|---:|---:|---:|---:|---:|---|
| native_full_34 | 25798 → 29066 | +12.67% | 14.787 → 12.862 | 71.78 → 64.89 | 54.00 → 50.63 | pass |
| h2_64k | 9748 → 10518 | +7.89% | 28.840 → 24.817 | 192.34 → 179.34 | 61.72 → 66.53 | pass |

Hoàn tất 1,819,455 request trước / 1,889,596 request sau; lỗi đã ghi nhận: 0. Stream chưa hoàn tất tại deadline: 2,560 (tách riêng, không tính thành request thành công).

Image trước: `sha256:4b1e9ae90ceecb4391ea31f5c52981ac70543311376b5fe5227416d1da532d66`; sau: `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`.

Config SHA256 trước/sau: `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0` / `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0`.

Lượt đối chứng này vẫn dùng đúng hai binary ban đầu, trước khi sửa layout. Mức giảm ở phép đo hỗn hợp ban đầu không tái hiện ổn định ở lượt focused này. Có container ngoài dự án đang chạy (mẫu docker stats khoảng 9–36% CPU); giữ nguyên container đó, ghi nhận ở `test-results/http-limits-focused/context.json`. Không lấy việc kết quả đảo dấu làm bằng chứng đã giải quyết nguyên nhân regression.

## Code mới, config mặc định — fixed request

Raw: `test-results/http-limits-default-fixed/`; 3 rounds, quota request cố định.

| Workload | RPS trước → sau | Δ RPS | p99 ms trước → sau | CPU µs/request trước → sau | Peak cgroup WS MiB trước → sau | Gate |
|---|---:|---:|---:|---:|---:|---|
| native_full_34 | 26933 → 31287 | +16.17% | 11.643 → 8.610 | 63.55 → 54.17 | 37.18 → 35.41 | pass |
| h2_64k | 10599 → 11146 | +5.16% | 19.490 → 16.743 | 176.74 → 159.74 | 44.21 → 43.44 | pass |
| tls_h1_64k | 10609 → 11885 | +12.03% | 14.875 → 19.478 | 147.09 → 146.03 | 41.75 → 43.09 | CHECK |
| h2_upload_1m | 160 → 160 | +0.22% | 406.473 → 408.534 | 3935.28 → 4033.49 | 36.94 → 37.09 | pass |
| grpc_stream | 1673 → 1672 | -0.02% | 46.484 → 47.173 | 978.37 → 971.15 | 37.07 → 36.84 | pass |

Hoàn tất 159,000 request trước / 159,000 request sau; lỗi đã ghi nhận: 0. Stream chưa hoàn tất tại deadline: 0 (tách riêng, không tính thành request thành công).

Vượt gate ở `tls_h1_64k`. Giữ kết quả này để điều tra; không coi là đã đạt toàn bộ gate.

Image trước: `sha256:4b1e9ae90ceecb4391ea31f5c52981ac70543311376b5fe5227416d1da532d66`; sau: `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`.

Config SHA256 trước/sau: `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0` / `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0`.

## Code mới, config mặc định — timed

Raw: `test-results/http-limits-default-timed/`; 3 rounds, 10 giây/lượt.

| Workload | RPS trước → sau | Δ RPS | p99 ms trước → sau | CPU µs/request trước → sau | Peak cgroup WS MiB trước → sau | Gate |
|---|---:|---:|---:|---:|---:|---|
| native_full_34 | 33954 → 28843 | -15.05% | 11.215 → 11.929 | 58.01 → 68.63 | 52.38 → 51.95 | CHECK |
| h2_64k | 12007 → 9801 | -18.37% | 17.620 → 22.434 | 166.22 → 203.71 | 60.44 → 61.98 | CHECK |
| tls_h1_64k | 12201 → 11570 | -5.17% | 21.499 → 18.564 | 158.19 → 167.29 | 57.84 → 56.19 | CHECK |
| h2_upload_1m | 141 → 144 | +2.27% | 463.390 → 445.445 | 4447.89 → 4433.27 | 48.23 → 40.43 | pass |
| grpc_stream | 1568 → 1676 | +6.85% | 60.409 → 56.944 | 1084.89 → 1114.81 | 50.30 → 41.46 | pass |

Hoàn tất 1,790,117 request trước / 1,554,847 request sau; lỗi đã ghi nhận: 0. Stream chưa hoàn tất tại deadline: 2,904 (tách riêng, không tính thành request thành công).

Vượt gate ở `native_full_34`, `h2_64k`, `tls_h1_64k`. Giữ kết quả này để điều tra; không coi là đã đạt toàn bộ gate.

Image trước: `sha256:4b1e9ae90ceecb4391ea31f5c52981ac70543311376b5fe5227416d1da532d66`; sau: `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`.

Config SHA256 trước/sau: `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0` / `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0`.

## Cùng binary, bật toàn bộ policy — timed

Raw: `test-results/http-limits-policy-timed/`; 3 rounds, 10 giây/lượt.

| Workload | RPS trước → sau | Δ RPS | p99 ms trước → sau | CPU µs/request trước → sau | Peak cgroup WS MiB trước → sau | Gate |
|---|---:|---:|---:|---:|---:|---|
| native_full_34 | 32457 → 31932 | -1.62% | 10.667 → 10.308 | 61.06 → 62.09 | 51.27 → 53.43 | pass |
| h2_64k | 10738 → 11133 | +3.68% | 20.240 → 17.123 | 186.01 → 179.51 | 62.70 → 61.57 | pass |
| h2_upload_1m | 141 → 144 | +2.63% | 471.137 → 447.002 | 4313.35 → 4407.27 | 57.73 → 55.98 | pass |
| grpc_stream | 1729 → 1748 | +1.09% | 52.296 → 51.847 | 1073.60 → 1050.17 | 60.80 → 57.93 | pass |

Hoàn tất 1,353,824 request trước / 1,336,127 request sau; lỗi đã ghi nhận: 0. Stream chưa hoàn tất tại deadline: 2,304 (tách riêng, không tính thành request thành công).

Image trước: `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`; sau: `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`.

Config SHA256 trước/sau: `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0` / `026bf9174b23e7df18844b715dec681f9fe58efdcb1ea904da5cb530fadf5ac8`.

Policy bật: upload tối đa 8 MiB; idle upload/response-body, response headers và socket write hai phía 5 giây; header-read 5 giây. Request nhỏ rỗng không tạo upload state. Upload 1 MiB đi qua bộ đếm/timer streaming; không buffer cả body.

## RSS và thu hồi tài nguyên

3 round, mỗi profile 10,000 HTTP + 1.000 TLS request, 128 idle clients, 32 TLS streams 16 MiB bị đọc chậm, 10 lần apply, hủy client rồi chờ pool expire. Process mới mỗi profile; không heaptrack/allocator override.

| Stage | RSS trước MiB | RSS mới mặc định MiB | RSS bật policy MiB |
|---|---:|---:|---:|
| startup | 9.16 | 9.16 | 9.34 |
| after_load | 20.20 | 20.77 | 20.56 |
| idle_clients | 20.36 | 20.93 | 20.88 |
| slow_tls_streams | 27.57 | 28.54 | 28.19 |
| after_10_updates | 27.77 | 28.77 | 28.34 |
| after_cancel | 23.66 | 24.23 | 23.51 |
| after_pool_expiry | 23.66 | 24.23 | 23.51 |

RSS là median giữa các process; allocator có thể giữ trang sau hủy nên không suy diễn RSS phải giảm ngay. Harness assert connection cuối bằng 0 và live generation bằng 1.

- `before`: FD cuối 24; generations cuối [1, 1, 1]; image `sha256:4b1e9ae90ceecb4391ea31f5c52981ac70543311376b5fe5227416d1da532d66`.
- `after`: FD cuối 24; generations cuối [1, 1, 1]; image `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`.
- `after_limits`: FD cuối 24; generations cuối [1, 1, 1]; image `sha256:dd0d9defb4793c2c599a4df7226d9632e8d3477d14c0274738d8645f437e86a8`.

Raw RSS/FD/metrics/status theo từng stage ở `test-results/http-limits-memory/`. Kiểm thử và ngữ nghĩa cấu hình: [http-limits.md](http-limits.md). Không đo chính xác từng H2 stream bị kẹt WINDOW_UPDATE bằng socket write timeout; không dùng kết quả này để tuyên bố đã có deadline đó.

## CPU profile để điều tra regression

Lấy mẫu `cpu-clock:u` 199 Hz, workload H2 64 KiB, ngoài lượt benchmark. Perf ghi trực tiếp lên `/mnt/d` bị lỗi Bad address; chạy lại trên filesystem Linux rồi copy artifact thành công. Bản perf của WSL thiếu libunwind/libdw và không giải mã ELF; dùng symbol table của đúng image ID để đối chiếu leaf IP. Không có call stack/inline đầy đủ; tỷ lệ dưới đây là self samples gần đúng, không phải CPU/request.

| Vùng CPU | Trước | Sau |
|---|---:|---:|
| `_aesni_ctr32_ghash_6x` | 16.92% | 16.59% |
| `prost_core::RunningProxy::handle_http::{{closure}}::{{closure}}` | 6.22% | 6.09% |
| libc tổng | 26.75% | 26.69% |

Hai IO worker đều gần bão hòa trong các lượt timed. Profile ban đầu chưa định vị được nguyên nhân chính xác: phân bố self samples không lộ một hàm mới chiếm tỷ trọng lớn. Không suy diễn rằng vì vậy không có regression. Các hàng CHECK giữ nguyên ý nghĩa cho đúng image và lượt đo tương ứng; thay layout và đo lại được báo riêng ở đầu tài liệu khi có kết quả.

Raw: `test-results/http-limits-cpu-linux/`; công cụ: `scripts/profile-http-cpu.py` và `scripts/symbolize-http-cpu.py`. Throughput của lượt có profiler không được dùng trong các bảng benchmark.
