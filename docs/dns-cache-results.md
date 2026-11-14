# Kết quả DNS/cache local Docker/WSL

Ngày kiểm tra: 14/11/2026. Đây là kiểm chứng chức năng trên Linux trong Docker/WSL,
chưa đo throughput/p99 và chưa xác nhận production cho image mới.

Release tag `prost:native` đã build thành công, WASM mặc định tắt:

```text
sha256:02d515726f09db7e517920782450a02001a06c5834722064b787459b575a4a7d
```

## Kiểm tra mã nguồn

`cargo fmt --all`, Clippy mọi target/feature với `-D warnings`,
`cargo test --workspace --locked` và bộ `--all-features` đều qua.
Mỗi cấu hình có **194 test passed, 0 failed**. Một test được đánh dấu ignored
là `storage_tests::crash_fixture`: helper subprocess được test
`crash_boundaries_restart_with_authoritative_config` gọi riêng, không phải
phần crash recovery bị bỏ kiểm tra.

Các test mới bao phủ:

- TTL cấu hình, negative TTL, timeout, singleflight, giới hạn hostname/IP,
  nhiều địa chỉ và DNS revision chỉ đổi khi tập địa chỉ đổi.
- Đổi HTTP/1.1 và HTTP/2 pool khi DNS đổi; stream cũ vẫn đọc hết sau khi
  client/pool cũ được thả; negative answer không dùng lại idle pool cũ.
- ETag/Last-Modified, metadata 304, 200 thay body, private/no-store bỏ entry cũ,
  stale deadline và các directive không cho phép stale.
- Purge trong khi validation chờ 304 không làm entry cũ xuất hiện lại;
  lease của body đang gửi vẫn được tính memory budget.
- Stale fallback khi backend trả lỗi/đóng transport vẫn cập nhật metrics và
  passive health, đồng thời cache còn dùng được khi endpoint bị eject.

Raw log: `test-results/dns-cache-checks.log`.
Build log/status: `test-results/dns-cache-release.log`,
`test-results/dns-cache-runner.json`.

## Daemon/CLI release thực

`scripts/dns-cache-smoke.py` đã qua trên đúng image hash ở trên. Fixture Python
độc lập, CA/leaf riêng, hai backend và network/container UUID được dọn sau test.
Thay answer `getaddrinfo` bằng `/etc/hosts` bên trong container proxy kiểm chứng:

- Request HTTP plain, HTTPS upstream có xác minh certificate và TCP dial mới
  sang backend B; HTTP stream và TCP session đang chạy vẫn ở A.
- Hostname không resolve được trả lỗi; hot apply khôi phục traffic và generation.
- Counter origin xác nhận cache hit, conditional headers, hai lần revalidation
  304, 200 cập nhật representation và stale-on-error. must-revalidate và purge
  chặn việc phục vụ stale.
- Restart giữ generation/config đã commit, khởi tạo lại cache/DNS RAM state.

Raw kết quả: `test-results/dns-cache-final/summary.json` và `commands.json`.
Trước purge: `revalidations=2`, `stale_hits=1`, `active_captures=0`.

Bộ `scripts/cache-control-smoke.py` cũng qua trên cùng image: origin nhận đúng
7 request qua các bước hit/miss, encoding variants, purge exact/all,
generation conflict, hot apply và restart.
Raw kết quả: `test-results/dns-cache-admin-regression/summary.json`.

## Giới hạn

TTL là policy cho resolver hệ thống, chưa đọc authoritative TTL/SRV.
General Vary, stale-while-revalidate và cache disk vẫn chưa hỗ trợ.
Soak 24 giờ đang chạy trên image cũ `89da44445826…` không bao phủ release này;
cần chạy đủ thời gian trên image triển khai trước khi kết luận production.

Cấu hình và ngữ nghĩa chi tiết: [dns-cache.md](dns-cache.md).
