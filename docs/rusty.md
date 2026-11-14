# Rusty: điều kiện thay HAProxy và giới hạn hiện tại

Các khả năng dưới đây đã được triển khai trong source. Kiểm chứng release và
resource fixture ghi ở cuối tài liệu. Chưa xác nhận có thể chuyển toàn bộ traffic
production của Rusty sang Prost; cần chạy với service/certificate thật và soak.

Ví dụ đại diện SMTP, IMAPS và HTTPS: [rusty-mail.toml](../examples/rusty-mail.toml).
Ví dụ không phải bản chuyển đổi toàn bộ HAProxy: cần thay public SNI/certificate
theo crt-list thật, bổ sung các listener còn lại và rà chính sách HTTP.

## PROXY v2 và IP client

```toml
[upstreams.imap]
send_proxy_v2 = true
```

Mỗi kết nối backend bắt đầu bằng header binary PROXY v2/TCP, **trước** ClientHello
TLS và trước application data. Có IPv4/IPv6, source/destination port. Trusted
ingress PROXY v1/v2 được nối tiếp với cả địa chỉ nguồn và đích; không lấy IP từ
HTTP X-Forwarded-For. Không có TLV SSL bổ sung hoặc PROXY v1 egress.

Health check mặc định kế thừa send_proxy_v2; header của probe dùng địa chỉ TCP
thật của probe, không mượn IP client. `health_check.send_proxy_v2=false` cho phép
port kiểm tra riêng không dùng PROXY. Backend yêu cầu PROXY cho mọi connection
nên giữ kế thừa, tương ứng check-send-proxy.

HTTP pool có PROXY được tách theo **frontend connection và upstream tương thích**.
HTTP/2 streams trên cùng frontend có thể tái sử dụng backend; hai frontend không
chia sẻ socket đã mang một IP khác. Weight/enable tương thích giữ pool; đổi
endpoint/TLS/cipher/identity tạo pool mới. Client gửi header XFF giả không đổi
metadata PROXY. Không bật PROXY cho upstream không có parser tương ứng.

`runtime.max_proxy_connections` (mặc định 4096, restart để đổi) giới hạn tổng TCP
session có send_proxy_v2 và socket HTTP có PROXY, kể cả connect/idle/draining.
Probe có budget health riêng. Hết budget từ chối ngay, không thêm queue; overload
cục bộ không làm endpoint bị passive eject. HTTP scoped pool giữ tối đa một
idle socket/host/frontend, idle tối đa min(pool_idle_timeout_ms,5000), tối đa 128
upstream cache entry/frontend. In-flight socket vẫn bị giới hạn bởi budget.
Metrics: prost_proxy_connections_active, prost_proxy_connections_limit.

PROXY header đứng trước TLS nên không nằm trong transcript mTLS. Backend vẫn cần
giới hạn nguồn kết nối tới các edge được tin cậy như mô hình HAProxy hiện tại.

## Health check mail

| protocol | Điều kiện đạt |
|---|---|
| connect (mặc định) | TCP và TLS nếu có; path cũ vẫn kích hoạt HTTP GET |
| http | GET path (mặc định /), đúng expected_status |
| smtp | Banner SMTP 220 hợp lệ, hỗ trợ multiline |
| imap | Banner * OK |
| pop3 | Banner +OK |
| smtp_starttls | Banner → EHLO có STARTTLS → STARTTLS/220 → verified TLS → EHLO/250 → QUIT/221 |
| imap_starttls | Banner → CAPABILITY có STARTTLS → tagged STARTTLS OK → verified TLS → tagged NOOP OK |
| pop3_starttls | Banner → CAPA có STLS → STLS/+OK → verified TLS → NOOP và QUIT/+OK |

STARTTLS chỉ là chức năng **health check**, không tự nâng cấp traffic mail. Client
mail vẫn có thể tự STARTTLS xuyên qua relay TCP. Backend mở cổng nhưng câm, thiếu
capability, từ chối upgrade, cert/hostname sai hoặc treo sau TLS đều làm probe
thất bại trong timeout_ms tổng. Không gửi email hoặc đăng nhập hộp thư khi probe.
Giới hạn 64 reply line/command, 512 byte/SMTP line, 4096 byte/IMAP/POP line và
64 KiB tổng reply/probe; không đọc reply không giới hạn.

```toml
[upstreams.smtp.health_check]
protocol = "smtp_starttls"
host = "edge.test" # EHLO name; không phải hostname xác minh certificate.
timeout_ms = 3000
fall = 3
rise = 2
# port = 1587 # Nếu probe khác port traffic, vẫn dùng host của từng endpoint.
[upstreams.smtp.health_check.tls]
server_name = "be-smtp"
ca_certificate = "/run/rusty-certs/internal-ca.crt"
[upstreams.smtp.health_check.tls.identity]
certificate = "/run/rusty-certs/edge-client.pem"
private_key = "/run/rusty-certs/edge-client.pem"
```

TLS của probe dùng health_check.tls nếu có, nếu không kế thừa upstream.tls.
STARTTLS bắt buộc có một trong hai, không có chế độ bỏ xác minh. CA/identity riêng
cũng được stage, giữ qua GC khi generation còn sống và khôi phục sau restart.

Trong HAProxy được cung cấp, bốn backend *_starttls chỉ expect banner, không gửi
lệnh STARTTLS và không cấu hình ssl cho traffic edge→backend. mTLS được khai báo
ở ba backend *_tls và http_tls. Không coi tên backend là bằng chứng mọi liên kết
đã dùng mTLS. Muốn mTLS cho cả các luồng clear cần thiết kế endpoint TLS tương ứng.

## mTLS và chính sách TLS

```toml
[upstreams.imap.tls]
server_name = "be-imap"
ca_certificate = "/run/rusty-certs/internal-ca.crt"
[upstreams.imap.tls.identity]
certificate = "/run/rusty-certs/edge-client.pem"
private_key = "/run/rusty-certs/edge-client.pem"
[upstreams.imap.tls.policy]
min_version = "tls12" # tls12 hoặc tls13; không hỗ trợ TLS 1.0/1.1.
session_resumption = false
cipher_suites = ["TLS13_AES_128_GCM_SHA256", "TLS13_AES_256_GCM_SHA384", "TLS13_CHACHA20_POLY1305_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"]
```

Policy tương tự dùng ở listeners.tls.policy và health_check.tls.policy. Danh sách
trên ánh xạ các suite trong file HAProxy, khác cách viết tên TLS13 của OpenSSL.
Không khai báo cipher_suites giữ mặc định ring: AEAD, ECDHE ở TLS1.2 và TLS1.3;
không CBC, DHE hữu hạn hoặc RSA key exchange. TLS1.2 mặc định còn có ECDHE-ChaCha,
nên muốn khớp allowlist HAProxy phải khai báo rõ như ví dụ.

session_resumption=false tắt toàn bộ resumption/ticket phía client và server,
mạnh hơn chỉ no-tls-tickets (không giữ TLS1.2 session-ID cache). Certificate/key
có thể ở hai file hoặc cùng combined PEM. Cấu hình CA riêng chỉ tin CA đó, không
cộng thêm public web roots. Không có CA riêng dùng webpki roots mặc định.

## SNI, timeout và admission

TLS termination đã có reject_unknown_sni=true và certificates[].server_names;
thiếu/không khớp SNI bị từ chối, wildcard chỉ một label. Default certificate
không biến thành fallback khi bật strict. tls_handshake_timeout_ms giới hạn
handshake, mặc định 5000 ms; max_connections_per_ip áp dụng trước handshake.

```toml
[[listeners]]
id = "opaque"
address = "0.0.0.0:10994"
protocol = "tcp"
upstream = "opaque_backend" # upstream plain TCP, backend tự TLS.
tls_passthrough = true
max_connections_per_ip = 50
[listeners.tls_passthrough_settings]
client_hello_timeout_ms = 3000
max_client_hello_bytes = 65536
reject_unknown_sni = true
server_names = ["mail.test"]
```

Passthrough dùng rustls Acceptor chỉ để đọc ClientHello rồi phát lại nguyên byte,
không terminate TLS, không route backend theo SNI. Default guard 5 giây/64 KiB;
strict là opt-in. Không thấy ClientHello, gửi dở, quá byte budget hoặc SNI sai sẽ
đóng frontend trước khi kết nối backend. Policy version/cipher/cert của phiên
opaque do backend thực thi; guard không xác minh toàn bộ handshake mã hóa.

upstream.connect_timeout_ms bao gồm DNS/TCP/PROXY/TLS cho implicit TLS.
idle_timeout_ms áp dụng relay, write_timeout_ms/client_write_timeout_ms cho phía
ghi, half_close_timeout_ms giới hạn drain sau FIN đầu tiên (0/không khai báo giữ
semantics cũ). Session còn dữ liệu đối chiều vẫn drain trong budget FIN.

max_connections_per_ip dùng IP socket hoặc trusted ingress PROXY, không XFF.
Counter chia sẻ worker và qua hot apply, kể cả connection nhận lúc limit=0;
entry được xóa khi hết connection. IPv4-mapped IPv6 được gộp với IPv4. Hết cap
đóng connection, không queue. Áp dụng ở cả TCP và HTTP, không đếm HTTP2 stream.

```toml
[[listeners.plugins]]
name = "connection_rate"
config = { connections_per_second = 10, burst = 100, key = "$remote_addr", max_keys = 100000, idle_timeout_ms = 30000 }
```

connection_rate native dùng GCRA, chạy trước TLS ở cả TCP/HTTP. Config không đổi
giữ rate debt qua apply; key chỉ global hoặc $remote_addr. Table giới hạn,
hết capacity từ chối ngay; idle eviction không xóa rate debt chưa trả.
10 conn/s + burst100 **không tương đương chính xác** HAProxy conn_rate(10s)>100;
cần chọn lại rate/burst theo tải. Đây là khác biệt còn lại khi chuyển policy.

## Kiểm chứng và điều kiện triển khai

Test core: binary PROXY v2 đọc bởi backend độc lập trước mTLS, IPv4/IPv6, hai H2
client không chia sẻ IP/socket, pool reuse khi đổi weight, local socket budget
không eject backend, backend từ chối thiếu client cert; SMTP STARTTLS lỗi/câm/
thiếu capability/treo sau TLS, sai hostname, restart từ artifact; IMAP/POP3
banner và STARTTLS qua mTLS; strict SNI termination/passthrough; timeout trước TLS;
per-IP cap, GCRA hot update; TCP half-close. Full suite native/all-features và
clippy chạy local Docker/WSL; kết quả cuối ghi ở [rusty-results.md](rusty-results.md).

Fixture release độc lập và workload 10.000 request:

```sh
sudo docker build -t prost:rusty-backend benchmarks/rusty
sudo python3 scripts/rusty-smoke.py --image prost:native --requests 10000 \
  --output-dir test-results/rusty-final-smoke
```

Fixture dùng certificate RSA tự tạo riêng, backend Go bắt buộc PROXY trước mTLS,
16 IP client, 10 hot update; soát byte/IP, TLS legacy/CBC/DHE và SNI âm tính.
Đây không phải Rusty thật và không phải benchmark capacity.

Chưa phải thay thế nguyên trạng toàn bộ HAProxy: template chỉ có ba listener đại
diện; unknown HTTP Host trả 404 theo routing thay vì 421 trong HAProxy; chưa có
policy HTTP keepalive 30s tương đương chính xác, conn-rate cửa sổ trượt, stick-table
CLI hoặc retry connect TCP như defaults retries2. Không áp dụng HTTP retry cho
phiên SMTP/IMAP. Cần giữ những khác biệt này trong tiêu chí cutover.

Wire format theo [PROXY protocol của HAProxy](https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt),
SMTP upgrade theo [RFC 3207](https://www.rfc-editor.org/rfc/rfc3207.html).
