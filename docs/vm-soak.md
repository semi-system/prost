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

```sh
export PROST_IMAGE='ghcr.io/semi-system/prost:sha-REPLACE_WITH_FULL_COMMIT_SHA'
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
