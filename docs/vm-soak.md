# Chạy image trên VM Linux để soak liên tục

Dùng tag theo commit hoặc digest được workflow publish. Giữ nguyên image trong
suốt đợt test để đối chiếu lỗi/tài nguyên; không tự cập nhật `latest` giữa chừng.
Workflow build Linux amd64, native, WASM tắt.
Runtime là Baseless static và chạy UID/GID 10001; không có shell/package manager.
Gọi quản trị bằng `docker exec prost prostctl ...`. Chi tiết các gate publish:
[image-security.md](image-security.md).

Nếu package GHCR private, đăng nhập bằng tài khoản có quyền đọc package và token
`read:packages`. Truyền token qua stdin, không ghi token vào command history hoặc
file config của Prost:

```sh
read -rsp 'GHCR token: ' GHCR_TOKEN; echo
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u YOUR_GITHUB_USER --password-stdin
unset GHCR_TOKEN
```

Chuẩn bị `prost.toml` với địa chỉ backend thật trên VM/network của bạn. Config mẫu
trong image dùng hostname `blue`/`green` của demo Compose; chúng không tự tồn tại
khi chạy một container riêng. Với TLS, chuẩn bị nguồn file/HTTP(S)/S3 tương ứng;
file mount phải đọc được bởi UID/GID 10001. S3 dùng role hoặc credential provider
đã mô tả trong [tls-sources.md](tls-sources.md).

Khởi đầu bằng [examples/vm.toml](../examples/vm.toml): copy thành `prost.toml`,
đổi `127.0.0.1:8081` thành backend thật. Ví dụ này nhận HTTP ở cổng 8080 và xuất
metrics tại loopback 9090 trên VM khi dùng host network.

```sh
# Image đã publish từ commit 42995a2; giữ digest này suốt đợt test.
export PROST_IMAGE='ghcr.io/semi-system/prost@sha256:56d6bba59208b274579cace0e4f411288a9778d4086ae983d93c7dafd4c01737'
docker pull "$PROST_IMAGE"

sudo mkdir -p /srv/prost
sudo install -m 0640 -o 10001 -g 10001 ./prost.toml /srv/prost/seed.toml

docker run -d --name prost --restart unless-stopped \
  --network host --stop-timeout 35 \
  --mount type=volume,source=prost-state,target=/var/lib/prost \
  --mount type=bind,source=/srv/prost/seed.toml,target=/etc/prost/seed.toml,readonly \
  --log-opt max-size=20m --log-opt max-file=5 \
  "$PROST_IMAGE"

docker exec prost prostctl status
docker inspect prost --format '{{.Image}}'
docker logs --tail 100 prost
curl --fail --show-error http://127.0.0.1:8080/
curl --fail --show-error http://127.0.0.1:9090/metrics
```

`--network host` dành cho VM Linux: cổng listener trong config được dùng trực tiếp
trên VM; kiểm tra chúng chưa bị service khác chiếm. Metrics nếu cấu hình
`127.0.0.1:9090` có thể được collector trên chính VM đọc. Admin API vẫn là Unix
socket trong container, gọi bằng `docker exec`.

Seed chỉ được Core nạp khi volume chưa có config; commit ban đầu cũng được
validate và lưu atomic dưới config lock. Sau đó daemon quản lý config trong
volume; sửa seed không tự kích hoạt cập nhật. Apply có validate và rolling drain:

```sh
docker exec prost prostctl validate --file /etc/prost/seed.toml
docker exec prost prostctl reload --file /etc/prost/seed.toml --wait --wait-timeout 60
```

Mount file giữ inode đã bind; nếu thay seed bằng atomic rename, mount mới file
candidate hoặc copy candidate vào container trước khi gọi validate/reload.
Giữ volume `prost-state` để restart khôi phục config/artifact đã commit. Mount
certificate local tại đúng path đã khai báo nếu dùng nguồn file.

Trong đợt test, lưu image digest/ID, config generation, log lỗi, CPU/RSS/FD/task,
health backend, latency và số request thất bại. Giới hạn CPU/RAM và workload nên
được giữ nhất quán giữa các lần đối chiếu. Thử restart container và reload config
trong lúc có stream dài; kiểm tra forced drain và generation cũ được thu hồi.
Nếu đổi `drain_seconds`, chỉnh `--stop-timeout` lớn hơn deadline drain tương ứng.
`restart unless-stopped` tự phục hồi container khi process thoát, không thay thế
việc theo dõi lỗi và ghi nhận downtime.

Kết quả local hiện có: [rolling-validation.md](rolling-validation.md).

## Soak tự động với backend thử nghiệm

Chạy daemon không có traffic chưa phải soak. Bộ script sau tự tạo backend, TLS
fixture và tải hỗn hợp HTTP/1, HTTPS/HTTP/2, streaming, cache/compression, TCP qua
rolling reload, access-log rotate và artifact GC. Chúng chỉ dùng container/network
UUID riêng; không tác động container `prost` đang phục vụ backend thật ở trên.
Đây là kiểm thử với backend tổng hợp, cần bổ sung traffic và giao thức thực tế
trước khi kết luận về môi trường của bạn.

Cần checkout mã nguồn có phiên bản script mới này trên VM Linux, Python 3.11+,
OpenSSL, `h2load` (Ubuntu: gói `nghttp2-client`), và Docker Engine cgroup v2 chạy
trực tiếp trên VM. Chạy script bằng root để đọc `/proc` và cgroup của container;
không dùng Docker context trỏ sang daemon từ xa. Không cần cài Rust.

Từ thư mục repo, sau khi pull `PROST_IMAGE` như trên:

```sh
docker pull python:3.13-alpine
docker build -t prost:bundle-backend benchmarks/advanced
python3 scripts/prepare-advanced-bench.py

# Chạy ngắn trước; dùng thư mục output mới cho mỗi lần chạy.
sudo python3 scripts/soak.py --image "$PROST_IMAGE" \
  --duration 60 --interval 5 --edge-policies \
  --output-dir test-results/vm-soak-check

# Chạy nền, tiếp tục khi ngắt SSH; launcher khóa ID của cả ba image.
sudo python3 scripts/start-soak.py --image "$PROST_IMAGE" \
  --cert-dir test-results/advanced-fixtures/certs \
  --duration 86400 --interval 10 --concurrency 16 --pause-ms 20 \
  --edge-policies --output-dir test-results/vm-soak-24h

cat test-results/vm-soak-24h/progress.json
tail -f test-results/vm-soak-24h/run.log
```

Launcher trả PID và lệnh `kill -TERM <pid>` để dừng đúng chiến dịch, giữ lại log
và trạng thái failed/interrupted. Giữ VM và Docker hoạt động đủ thời gian; chưa
tự khởi chạy lại chiến dịch sau khi reboot. `summary.json` có `state: "passed"`
mới xác nhận hoàn tất; launcher thành công hoặc progress đang chạy chưa phải đạt.

Tải H1 dùng connection mới cho mỗi request, không tự retry khi lỗi. Probe policy
riêng kiểm tra keepalive sau idle. Mỗi reload giữ một phiên TCP qua commit rồi
đóng trước deadline; barrier `workers wait` xác nhận worker cũ đã drain trước
lần reload tiếp theo. Phiên TCP không được giữ qua mọi generation vì rolling
update chủ động đóng nó khi hết thời hạn drain. Lần chạy này yêu cầu 0 forced
drain và 1 generation còn sống sau mỗi barrier.

Proxy được giới hạn 2 CPU/256 MiB, backend 2 CPU/256 MiB; helper chẩn đoán riêng
0,25 CPU/64 MiB. Helper dùng cùng UID để rotate file trong volume, không thêm shell
hay công cụ vào image production. Script lấy mẫu CPU cgroup, RSS, FD/task, status
và tổng traffic vào `samples.jsonl`, ghi metrics cuối vào `final-metrics.prom`.
RSS/FD cao nhất trong summary là đỉnh **đã lấy mẫu sau drain**, không bao gồm mọi
đỉnh tức thời trong khoảng lấy mẫu. Các gate RSS dưới 192 MiB, FD/task dưới 512,
không lỗi request/access log và không tích lũy generation là giới hạn của bộ
fixture này, chưa phải cam kết năng lực production. Tải tổng hợp có bật cache;
không dùng kết quả này làm so sánh throughput với nginx/HAProxy không bật cache.

Kiểm chứng local WSL ngày 15/11/2026: hai lượt 60 giây, một lượt trực tiếp và
một lượt chạy nền có `--edge-policies`, đều đạt trên image Baseless build local
`sha256:c290d642a25c821075b2ea0d4095920fd6ffa9c8945dfc8f9f6bae5aeb590278`
từ cùng mã nguồn release 42995a2. Đây là ID local, không phải digest registry ở
trên. Tổng 62.106 H1 request/0 lỗi, 2.200 H2 probe, 22 rolling reload và 44 TCP
probe; sau mỗi barrier còn 1 generation và không forced drain. RSS cao nhất đã
lấy mẫu 14,71 MiB, FD cao nhất 81. Policy xác nhận 12 Host denial trả 421 và
keepalive reuse sau idle. Raw: `test-results/baseless-rolling-soak-smoke` và
`test-results/baseless-rolling-soak-launcher`. Đây chưa phải kết quả soak 24 giờ
hay benchmark năng lực; không có thay đổi binary/image trong lần sửa script này.
