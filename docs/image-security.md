# Image Baseless và gate publish

Runtime mặc định dùng `ghcr.io/semi-system/baseless-static:latest` để nhận các
bản cập nhật base. Mỗi build release trên CI pull `latest`, ghi digest vào
workflow summary và truyền digest đó qua build arg `RUNTIME_IMAGE`; build và
scan dùng cùng bản base vừa lấy. Không cần sửa Dockerfile khi base cập nhật.
Bốn executable `prost`, `prostctl`, `prost-entrypoint`, `prost-certd` được build cho
`x86_64-unknown-linux-musl`. Image không thêm shell, package manager hay thư viện
Debian. UID/GID vẫn là 10001 để giữ tương thích volume đã tạo từ bản trước.
Chỉ các thư mục ứng dụng được đổi owner; `/etc`, `/usr` và inventory APK của base
vẫn giữ quyền gốc. CA bundle/timezone đến từ Baseless.

`prost-entrypoint` dùng exec, giữ daemon ở PID 1 và chuyển nguyên tín hiệu/exit
code. Không truyền command thì nó chạy daemon với managed config và `--seed`.
Core chỉ dùng seed khi managed config chưa tồn tại, dưới config lock; validate,
prepare, bind và atomic persistence xong mới khởi chạy listener. Startup lỗi
không để lại managed config dở. Config đã tồn tại luôn có thẩm quyền, kể cả
khi sai; daemon không tự ghi đè nó bằng seed.

Build dùng `cargo-auditable` 0.7.7 với lockfile của tool để nhúng dependency graph
vào từng executable. Vì vậy Trivy vẫn quét được các crate trong binary static;
không dựa vào việc vắng package manager để coi application sạch. Prometheus chỉ
bật text exposition, bỏ parser protobuf không dùng và CVE-2025-53605 của
dependency cũ đó. Định dạng metrics HTTP vẫn giữ nguyên.

Luồng publish:

1. Test workspace native/all-features và các protocol smoke phải đạt.
2. `abxst/actions/strict` (pin commit của v5) chạy Semgrep, build một image và
   Trivy. Vẫn chặn mọi severity, kể cả CVE chưa có bản vá.
3. `semi-system/baseless/scan` kiểm tra identity, APK inventory, version mapping
   và advisory feed Baseless trên chính image đã scan. Gate này bắt buộc vì
   generic Trivy chưa nhận diện Baseless như một distro OS được hỗ trợ.
   Workflow cũng yêu cầu report Trivy nhận diện dependency metadata của cả bốn
   Rust executable; report trống do thiếu audit metadata không được coi là đạt.
4. Chỉ sau khi cả hai gate đạt mới login/push các tag trỏ đến image đó; không
   build lại. Workflow kiểm tra image ID và digest giữa các tag.

Report Semgrep/Trivy nằm trong artifact `image-security-reports`; report của
base trong `baseless-security-report`. Có thể lấy tag theo commit hoặc digest
từ workflow summary. Các action trực tiếp của workflow được pin bằng commit.
Thay base vẫn phải qua các gate và kiểm thử; giữ scanner/metadata tương thích.
Build local dùng `docker build --pull -t prost:mvp .` để lấy tag mới nhất.
Triển khai vẫn dùng digest của image Prost đã scan để giữ đúng bản đang chạy.

Các annotation `nosemgrep` trong benchmark/smoke chỉ áp dụng từng rule tại fixture
đã rà soát: HTTP loopback do CLI/operator kiểm soát, payload JSON/text/binary
được khai báo, negative TLS handshake, listener của mạng test riêng và image
heaptrack tùy chọn. Mỗi vị trí có lý do tại source. Không bỏ qua cả thư mục,
không có ignore CVE, và không hạ severity của gate.

Chẩn đoán container tối giản dùng `docker exec prost prostctl ...`, metrics và
`docker logs`; không có `sh`, `cat`, `mv`, `apk` hay `apt-get`. Fixture cần đọc
process/đổi DNS dùng helper riêng, không cài công cụ vào image production.
Đo RSS/FD/thread vẫn dùng được; heaptrack/LD_PRELOAD cần một profiling image GNU
liên kết động riêng, không dùng executable static của bản phát hành này.

Hướng dẫn VM: [vm-soak.md](vm-soak.md). Gate đạt không thay thế soak/workload
qualification và không phải cam kết không còn lỗ hổng chưa được biết đến.

## Kiểm chứng local — 2026-11-15

Image runtime được kiểm tra:
`sha256:c290d642a25c821075b2ea0d4095920fd6ffa9c8945dfc8f9f6bae5aeb590278`.
Đây là image local chứa cùng binary musl/auditable được build từ source; image
do CI build/publish có metadata và digest riêng, phải lấy từ workflow của commit.

- Rustfmt/Clippy all-targets/all-features đạt; 211 test đạt ở mỗi chế độ native
  và all-features. Test bootstrap xác nhận rollback khi bind lỗi, quyền 0600,
  restart từ managed config khi mất seed, không thay config sai và không ghi qua
  dangling symlink.
- Semgrep: 0 finding, 0 error sau các annotation fixture đã rà soát.
- Trivy: 0 vulnerability, nhận diện dependency của ba Rust binary (212 package
  mỗi binary theo metadata của package CLI); gate Baseless với require-current đạt.
- Smoke S3/SigV4/presign/file URI, TLS rotate/rollback/offline restart, DNS/cache,
  cache control, SNI và demo CLI đều đạt trên image này.
- Hai vòng × 10.000 HTTP request, 16 client và 20 rolling reload/vòng đều đạt,
  không lỗi và không retry; các phiên TCP qua commit và worker drain được kiểm tra.
- Actionlint và sáu Python parser contract test đạt.

Raw local được gitignore:
[workspace](../test-results/baseless-ship-checks/status.json),
[runtime](../test-results/baseless-ship-runtime/summary.json),
[Trivy](../test-results/baseless-ship-security/trivy-report.json),
[Baseless](../test-results/baseless-ship-security/baseless-report.json),
[Semgrep](../test-results/baseless-ship-security/semgrep-report.json).
Các báo cáo benchmark/soak của image GNU trước đây không đại diện cho runtime
static mới; cần tiếp tục soak trên đúng digest được publish cho VM.
