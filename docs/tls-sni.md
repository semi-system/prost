# TLS nhiều domain trên cùng listener

Prost chọn certificate bằng SNI trong ClientHello cho cả listener HTTP và TCP.
TLS upstream vẫn cấu hình độc lập. Routing HTTP tiếp tục theo Host/authority và
path; SNI chỉ chọn certificate, không tự tạo route hoặc ràng buộc Host với SNI.

```toml
[listeners.tls]
certificate = "/certs/default.pem"
private_key = "/certs/default.key"
reject_unknown_sni = false

[[listeners.tls.certificates]]
server_names = ["api.example.com", "api-alias.example.com"]
certificate = "/certs/api.pem"
private_key = "/certs/api.key"

[[listeners.tls.certificates]]
server_names = ["*.example.com"]
certificate = "/certs/wildcard.pem"
private_key = "/certs/wildcard.key"
```

Cấu hình đầy đủ với backend blue/green của Compose:
[examples/sni.toml](../examples/sni.toml). Mount certificate/key tương ứng vào
`/certs` và publish cổng 8443 trước khi dùng mẫu đó. File certificate là PEM với
leaf ở đầu, tiếp theo là intermediate chain; key phải khớp leaf certificate.
`server_names` chỉ định lựa chọn; client vẫn xác minh SAN/hostname của certificate.

## Quy tắc chọn

1. Exact domain được ưu tiên.
2. Wildcard trái nhất `*.example.com` khớp đúng một label: `shop.example.com`,
   không khớp `example.com` hoặc `deep.shop.example.com`.
3. Không khớp hoặc client không gửi SNI: dùng certificate mặc định.
4. Với `reject_unknown_sni = true`, bước 3 từ chối TLS handshake. Cần khai báo ít
   nhất một entry có server_names; tên trong SAN của default cert không tự thành
   entry được cho phép.

Domain không phân biệt hoa thường ASCII; dấu chấm cuối được chuẩn hóa bỏ đi.
Dùng punycode cho IDN. Không nhận regex, IP literal, wildcard ở giữa hoặc
wildcard một phần label. Không cho hai entry cùng tên sau chuẩn hóa. Exact và
wildcard chồng nhau được phép vì exact có ưu tiên rõ ràng.

Mỗi listener tối đa 64 bundle SNI và 256 tên, ngoài bộ certificate mặc định.
Tổng PEM của các cặp cert/key khác nhau được parse tối đa 16 MiB/listener.
Cặp đường dẫn giống nhau được parse một lần; trong daemon các file cùng nội
dung được quy về artifact hash. Các tên dùng chung bundle chia sẻ `Arc<CertifiedKey>`.
Hiện mỗi domain chọn một bundle, chưa chọn riêng RSA/ECDSA theo signature scheme.

## Apply và vòng đời

```sh
prostctl validate --file ./candidate.toml
prostctl apply --file ./candidate.toml --if-generation 12
```

Daemon đọc certificate/key, lưu artifact, kiểm tra PEM/key và dựng bảng chọn
trước commit. Name sai, trùng tên, file thiếu hoặc key không khớp làm apply thất
bại và giữ nguyên generation/config đang chạy. Không đọc file trên đường
handshake, không polling certificate. Tra bảng exact/suffix bằng hash; không
quét toàn bộ danh sách domain. SNI lowercase thông thường không cần tạo chuỗi mới.

TLS config không thay đổi được tái sử dụng qua cập nhật route/header/upstream,
giữ session cache hiện tại. Thay certificate hoặc policy SNI tạo ServerConfig
mới; phiên TLS đã mở tiếp tục với TLS state cũ. Kết nối được nhận và lấy snapshot
sau publish dùng cấu hình mới; handshake đã bắt đầu có thể giữ snapshot cũ.
Request/HTTP2 stream mới trên kết nối đang mở vẫn lấy route/plugin mới.

Sau restart daemon dùng artifact đã commit, không phụ thuộc file nguồn còn tồn
tại. Private key nằm trong artifact hạn chế quyền; export chỉ chứa đường dẫn.
Đây là TLS termination, chưa có TLS passthrough, mTLS hoặc tự cấp certificate ACME.

## Kiểm tra Docker trong WSL

```sh
sudo docker build -t prost:native .
sudo python3 scripts/sni-smoke.py --image prost:native
```

Smoke tự sinh certificate test và tạo Compose project riêng, kiểm tra HTTPS và
TCP/TLS, fallback/no-SNI, strict rejection, hot rotate, phiên cũ, rollback và
restart từ artifact. Cuối phép thử xóa project/volume test. Không đổi managed
config của Compose demo đang dùng.

Resolver dựa trên [rustls ResolvesServerCert](https://docs.rs/rustls/0.23.46/rustls/server/trait.ResolvesServerCert.html).
