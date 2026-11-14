# Kiểm chứng Rusty trên Docker/WSL

Bản release native (WASM tắt), Rust 1.93, Cargo.lock, thin LTO:
`sha256:a7e9c3e38f7dbd14bcdd6d2b2f6c3ee08e58ce6af74a9c13f97363d9faf7528a`.
Chức năng/cấu hình/giới hạn: [rusty.md](rusty.md),
[rusty-mail.toml](../examples/rusty-mail.toml).

## Source và giao thức

Format, clippy all-targets/all-features `-D warnings`, full native và full
all-features: **140 test đạt ở mỗi chế độ**. Một helper crash-test được test cha
gọi bằng subprocess, không phải ca bỏ kiểm chứng. Harness dùng bốn test thread,
hai build job. Raw: `test-results/rusty-final-checks.log`.

Tám test integration mới kiểm tra:

- Backend độc lập đọc PROXY v2 trước mTLS, giữ source/destination IPv4/IPv6.
- Hai H2 frontend có pool riêng, giữ IP/port đúng và reuse qua hot update weight.
- Cạn socket budget trả 503 mà không passive eject; đóng frontend trả socket.
- Backend mTLS từ chối khi bỏ identity; health check xác minh hostname/CA.
- SMTP STARTTLS thực hiện EHLO trước/sau TLS; loại câm/bad banner/thiếu STARTTLS/
  treo sau TLS/sai hostname và phục hồi. Restart vẫn chạy sau xóa file TLS nguồn.
- IMAP/POP3 banner qua implicit mTLS và STARTTLS/STLS có kiểm tra sau TLS.
- Passthrough replay nguyên byte, chặn SNI lạ/thiếu và ClientHello chậm/dở.
- TLS1.2/1.3 và cipher allowlist; handshake deadline và cap IP trước handshake;
  counter qua hot apply (kể cả lúc limit=0), GCRA giữ rate debt, half-close drain.

Unit test provider xác nhận chỉ TLS1.2/1.3 và các suite AEAD TLS13/ECDHE, không
CBC hoặc finite-field DHE. Ca legacy wire riêng nhận fatal alert; fixture release
bên dưới dùng OpenSSL thật để loại lỗi do tự dựng ClientHello.

## Release: 10.000 request qua PROXY v2 và mTLS

Proxy hai worker, giới hạn 2 CPU / 256 MiB. Backend Go riêng, cùng giới hạn,
**bắt buộc** binary PROXY v2 rồi verified client certificate. 16 HTTPS client
keepalive, mỗi client một IP/port PROXY đầu vào; soát từng response phải đúng IP.
CA và RSA leaf certificate riêng, verify hostname/chain bật. 10 hot apply trong
lúc tải. Mỗi request có pause 2 ms để cập nhật xen kẽ: đây là bài correctness và
tài nguyên, **không đo capacity tối đa** và không so sánh RPS với nginx.

| Kết quả | Giá trị |
|---|---|
| Request / traffic error | 10.000 / 0 |
| Hot apply | 10 |
| Thời gian traffic fixture | 2,418 giây |
| CPU proxy/request | 142,70 µs, gồm chi phí hot apply trong interval |
| Process RSS đỉnh lấy mẫu | 12,87 MiB |
| Cgroup working set đỉnh lấy mẫu | 10,45 MiB |
| FD đỉnh lấy mẫu | 58 |
| Sau đóng traffic | 0 connection, 1 live generation |

Có positive control xác minh CA/hostname và IP trước các ca âm tính. OpenSSL
gửi handshake TLS1.0, TLS1.1, ECDHE-RSA CBC và DHE-RSA GCM: cả bốn bị server từ
chối với fatal handshake alert. Không tính lỗi local NO_CIPHERS/NO_PROTOCOLS hay
lỗi certificate verification là bằng chứng từ chối cipher/protocol. SNI unknown
và absent cũng bị từ chối. Certificate RSA giúp bài DHE/CBC không thất bại chỉ
vì server dùng ECDSA key.

Raw: `test-results/rusty-final-smoke-v3/summary.json`, samples.json và container
logs; script tái hiện: `scripts/rusty-smoke.py`. Hai fixture trước đó thất bại
do dùng CA làm leaf, rồi do CA thiếu keyUsage dưới OpenSSL strict verification;
giữ log ở rusty-final-smoke và rusty-final-smoke-v2, không tính chúng là đạt.
Không hạ mức xác minh TLS để làm test qua.

Example qua `prost check` là **schema only**; TLS artifact của môi trường thật
cần `prostctl validate`. Chưa có certificate/crt-list thực tế hoặc truy cập
service Rusty của người dùng trong bài này.

## Độ ổn định và hiệu năng

Demo native headers/maps/rate/rewrite, balancing/backup/capacity/passive health,
HTTP/TCP, atomic apply và giữ phiên cũ: đạt. Smoke hỗn hợp 60 giây: 31.854 H1
request, 1.100 H2/TLS probe, 11 hot apply, không lỗi; RSS đỉnh 22,95 MiB, FD đỉnh
88. Hai generation khi còn TCP cũ; sau đóng còn một generation, 0 connection.
Raw: `test-results/rusty-final-soak-smoke/summary.json`.

Đối chứng default path, 3 round × 8 giây/image, pin client/proxy/backend lên các
core guest riêng, không build hoặc soak đồng thời. Image control là `prost:bundle-v2`,
`sha256:25a01bb041ecb86225abc718f7f88dbd611e49db952656c95f893775af38c642`.

| Workload | RPS control → candidate | p99 ms control → candidate | Gate |
|---|---|---|---|
| H2 TLS 64 KiB | 7.516 → 8.346 | 39,382 → 43,949 | p99 +11,60%, cần điều tra |
| Native header 34 B | 13.058 → 23.955 | 28,289 → 17,909 | Trong ngưỡng của lượt này |

Không có traffic failure. Timed unfinished stream báo riêng trong raw, không
đếm là response thành công. RPS control từng round của H2 là 4.814/7.516/9.351,
candidate 4.383/8.346/10.933; 34 B control 9.574/13.058/20.336, candidate
20.903/29.725/23.955. Dao động quá lớn để gán mức tăng median 83% của 34 B cho
thay đổi code. Pinning guest không chứng minh host đã cách ly. Không tuyên bố
capacity hay đã xử lý xong regression H2. Raw: `test-results/rusty-final-default-bench`.

Giữ riêng kết quả trước Rusty tại [mvp-bundle-results.md](mvp-bundle-results.md):
H2 p99 và logging có lượt vượt gate, chưa coi mục tiêu tối ưu là hoàn tất.

Đã khởi chạy soak 24 giờ lúc **13:16 ngày 14/11/2026, Asia/Saigon**, bằng image
ID cố định ở đầu báo cáo; dự kiến kết thúc khoảng 13:17 ngày 15/11 nếu host tiếp
tục hoạt động. Mẫu ở giây 98: 50.764 H1 request, 900 H2/TLS probe, 10 TCP probe
qua 9 apply, không lỗi; process vẫn chạy độc lập trong WSL.

Soak đủ 24 giờ chưa được xác nhận đạt. Theo dõi
`test-results/soak-24h-v2/progress.json`; chỉ state=passed kèm summary.json sau đủ
thời gian và kiểm tra cuối mới là kết quả hoàn tất. Lượt soak hỗn hợp không thay
thế thử SMTP/IMAP/POP3 dài hạn với Rusty thật.

Lượt đầu ở `test-results/soak-24h` dừng trước mẫu đầu: script chờ 10 giây mới
gửi request đầu qua TCP, bằng ReadHeaderTimeout của backend fixture. Đã giữ
trạng thái failed và log, sửa script để prime TCP ngay khi mở; không thay timeout
proxy để né lỗi. Kiểm tra lại interval 10 giây trong 30 giây đạt: 15.928 H1 request,
300 H2/TLS probe, 4 TCP probe qua 3 apply, không lỗi; raw
`test-results/rusty-soak-interval10-smoke/summary.json`. Lượt 24 giờ mới dùng thư
mục riêng `soak-24h-v2`, không ghi đè bằng chứng cũ. Cần giữ WSL/Docker hoạt động.

Hiện phù hợp MVP/pilot có giám sát. Các khác biệt với HAProxy (GCRA, HTTP Host
status/keepalive, retry TCP, listener/certificate mapping) được ghi rõ trong
[rusty.md](rusty.md); chưa xác nhận cutover production nguyên trạng.
