# Xoay frontend TLS và health check PROXY v2 LOCAL

`prostctl tls reload` thay TLS của một listener đang bật TLS mà không đổi worker
hoặc bắt đầu drain connection. Connection/handshake đã nhận dùng snapshot cũ;
connection mới nhận certificate, SNI map và TLS policy mới. Routing, upstream,
admission, socket và timeout không bị sửa bởi API này.

```sh
prostctl tls reload imaps_in --file ./tls-refresh.toml --if-generation 12
```

File là một đối tượng `ServerTls` đầy đủ, không có wrapper `[listeners.tls]`:

```toml
certificate = "file:///etc/prost/tls/mail/fullchain.pem"
private_key = "file:///etc/prost/tls/mail/privkey.pem"
reject_unknown_sni = true
[[certificates]]
server_names = ["mail.example.test"]
certificate = "file:///etc/prost/tls/mail/fullchain.pem"
private_key = "file:///etc/prost/tls/mail/privkey.pem"
[policy]
min_version = "tls12"
session_resumption = false
```

File/HTTP(S)/S3 dùng staging artifact như full apply. Ví dụ S3:
[tls-refresh.toml](../examples/tls-refresh.toml). `runtime.s3` và credential provider
lấy từ config hiện hành; update chỉ gửi TLS, không phải toàn bộ config.

Thao tác dùng cùng coordinator với apply/validate/GC. Generation kỳ vọng được
kiểm tra trước staging. Lỗi tải, cert/key, schema hoặc persistence trước rename
giữ nguyên runtime. Commit ghi config/artifact rồi publish snapshot đã chuẩn bị;
config generation tăng một, `worker_generation` giữ nguyên. Pool/health/admission
tương thích được tái sử dụng. Artifact cũ được GC bảo vệ trong khi phiên cũ giữ
snapshot. Client ngắt socket quản trị không hủy transaction đã nhận.

API v1 có method `tls.reload` với `listener`, `tls`, `expected_generation`.
Response gồm `generation`, `hash`, `durable`, `worker_generation`,
`worker_drain_started=false`. Không truyền `--if-generation` thì CLI đọc generation
trước khi ghi; vẫn có thể conflict nếu client khác commit. `durable=false` sau
directory-fsync failure có nghĩa đã commit, giống full apply; CLI báo lỗi vận hành,
không được retry mù. Metrics transaction dùng operation `tls_reload`.

API hiện chỉ đổi frontend TLS. Upstream TLS, protocol và route vẫn dùng full
rolling apply. Drain đã bắt đầu do một full apply trước đó vẫn tiếp tục; refresh
không hủy drain đó. Idle/write/half-close timeout vẫn có hiệu lực. Idle 300 giây
vẫn đóng IMAP không truyền dữ liệu quá giới hạn; cần idle budget phù hợp hoặc
heartbeat từ dịch vụ; `idle_timeout_ms=0` tắt idle cutoff cho TCP/IMAP.

Agent ACME HTTP-01 và batch API `tls.reload_many` được mô tả trong
[acme.md](acme.md). Certificate đổi trên nhiều listener trong một transaction,
giữ nguyên phiên đang chạy. Không có watcher S3 hoặc polling config.

## Health check LOCAL

```toml
[upstreams.mail]
send_proxy_v2 = true
[upstreams.mail.health_check]
protocol = "connect"
proxy_v2_command = "local"
```

Probe gửi 16 byte: signature PROXY v2, command `0x20`, family/protocol UNSPEC,
length 0, không address block hoặc TLV. Header đứng trước TLS nếu probe có TLS;
connect, HTTP và mail/STARTTLS checks dùng chung transport này. Traffic vẫn dùng
command PROXY (`0x21`), mang IP/port client và không TLV.

`proxy_v2_command` mặc định `proxy` để giữ tương thích backend hiện có. Trường này
chỉ áp dụng probe, không tự bật PROXY. Config `local` cùng effective
`send_proxy_v2=false` bị từ chối. Nếu port health riêng không có parser PROXY,
dùng `health_check.send_proxy_v2=false` và giữ command mặc định.

## Kiểm chứng

Test Docker/WSL bao phủ hot/rolling: TCP/TLS qua quá deadline drain, certificate
và SNI mới, conflict đồng thời, lỗi cert/key, lỗi persist, GC giữ artifact đang dùng
và restart khi mất file nguồn. Test Unix socket còn kiểm tra client ngắt kết nối
trước response và full apply sau TLS refresh.

Các integration test dùng decoder độc lập, kiểm tra IPv4/IPv6, header LOCAL,
không TLV, truyền nguyên byte và giữ phiên qua TLS refresh. Giao diện backend là
PROXY protocol v2 chuẩn; bản phát hành không phụ thuộc source của ứng dụng backend.

## Container non-root, read-only và low ports

Native Baseless image được kiểm tra thêm bằng:

```sh
sudo python3 scripts/mail-runtime-smoke.py --image prost:mail-next \
  --output-dir test-results/mail-runtime-smoke
```

`--image` phải chứa code của đợt này; image trên VM đã publish trước đó chưa có
`tls reload`/stdout sink. Bản local được test là
`sha256:e50306b1e997bf483cc1f760678128ef6caff3b64218cf51a16a4391339b76b9`.

Profile dùng Docker bridge, `--user 10001:10001`, `--read-only`, `--cap-drop ALL`,
`--security-opt no-new-privileges:true`,
`--sysctl net.ipv4.ip_unprivileged_port_start=0`. Sysctl cho phép bind 25/993 bên
trong network namespace mà không cần CAP_NET_BIND_SERVICE. Không áp dụng sysctl
này cùng `--network host`; với profile host network cần thiết kế port mapping hoặc
chính sách host phù hợp. `/run/prost` là tmpfs UID/GID 10001, mode 0700;
`/var/lib/prost` là data volume writable để commit config/artifact, seed/nguồn cert
mount read-only. Rootfs read-only không có nghĩa data volume cũng read-only.

Script đọc `/proc/1/status` và mountinfo của chính proxy để xác nhận UID 10001,
CapEff=CapBnd=0, NoNewPrivs=1, root mount `ro`; kiểm tra bind low ports, CLI TLS
refresh, rollback cert/key sai, giữ TCP/TLS quá drain deadline và ba session log
stdout. Fixture/backend/network UUID được dọn sau test. Chưa phải manifest
standalone đủ bảy cổng hoặc chứng minh các protocol mail thực tế.
