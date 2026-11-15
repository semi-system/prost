# TLS artifact: file, HTTP(S) và S3

Các trường `certificate`, `private_key`, `client_ca`, `ca_certificate` và identity
mTLS nhận đường dẫn hiện tại hoặc URI. Áp dụng cho default/SNI certificate,
upstream TLS và TLS health check. WASM module vẫn dùng file local.

```toml
[listeners.tls]
certificate = "https://certs.example.com/mail/fullchain.pem"
private_key = "file:///etc/prost/tls/mail.key"
```

`file:///absolute/path.pem` và `file://localhost/absolute/path.pem` chỉ đọc file
local; hỗ trợ percent encoding như `%20`. Relative path thông thường giữ cách
resolve theo working directory của daemon. `prostctl` gửi URI cho daemon, nên
file phải tồn tại trong filesystem/container của daemon.

HTTP(S) tải bằng GET, tối đa ba redirect; chặn HTTPS chuyển xuống HTTP và redirect
sang scheme khác. HTTPS xác minh CA/hostname bằng trust roots mặc định. URL có
userinfo hoặc fragment bị từ chối; query được hỗ trợ cho presigned GET URL.
Không tự đọc JSON từ API cấp link: endpoint cần trả PEM hoặc redirect sang PEM.
Một URL HTTPS ổn định trả 302/307 sang presigned URL mới là lựa chọn phù hợp khi
đã có service cấp link.

## S3 native

```toml
[runtime.s3]
region = "ap-southeast-1"
# S3-compatible: bỏ comment và cấu hình endpoint/region tương ứng.
# endpoint = "https://objects.example.com"
# force_path_style = true
# ca_certificate = "file:///etc/prost/object-store-ca.pem"

[listeners.tls]
certificate = "s3://proxy-certs/mail/fullchain.pem"
private_key = "s3://proxy-certs/mail/privkey.pem"
```

Ví dụ đầy đủ: [tls-s3.toml](../examples/tls-s3.toml).

Prost tự ký GET bằng SigV4, không cần presigned URL. Dùng credential provider
chính thức của AWS: environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`), shared profile, assume role/web identity, ECS hoặc EC2
IMDSv2 tùy môi trường. Không đưa access key/secret key vào TOML. Bản build này
không bật SSO hoặc credential-process. Credential được resolve khi tải artifact,
không nằm trên đường xử lý connection/request của proxy.

Region lấy từ `runtime.s3.region` hoặc provider chain AWS. Endpoint tùy chỉnh
phải là HTTP(S) origin; `force_path_style=true` phù hợp với MinIO/RustFS. CA riêng
cho endpoint S3 nhận local path/file URI, được kiểm tra và lưu vào artifact store.
CA này không thay trust roots của URL HTTP(S) thông thường hoặc của AWS STS.

Hỗ trợ general-purpose bucket, key UTF-8 tối đa 1024 byte và
`s3://bucket/path/cert.pem?versionId=...`. Percent encoding giữ danh tính object;
dấu `+` trong key là dấu cộng. Không normalize `..` hoặc gộp slash của key.
AWS endpoint mặc định dùng virtual host, chuyển path style cho bucket có dấu chấm;
có suffix cho AWS China. Chưa hỗ trợ ARN/access point/S3 Express/SigV4a.

Native S3 không follow redirect để tránh chuyển Authorization sang endpoint khác;
cần cấu hình đúng region. IAM principal cần quyền GET object; object version/KMS
có thể yêu cầu quyền bổ sung. Khóa riêng nên dùng file hoặc HTTPS/S3 endpoint có TLS;
HTTP truyền nội dung artifact không mã hóa.

Tham chiếu: [AWS credential chain](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html),
[presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).

## Vòng đời và giới hạn

Tải trong startup, validate hoặc apply; không polling URL/S3 và không tải trong
TLS handshake. Mỗi lần staging dùng một cache URI để nguồn trùng nhau chỉ tải
một lần. Giới hạn: 16 MiB/artifact, 32 MiB remote payload/transaction, 128 remote
source, 10 giây cho một lượt tải (gồm redirect/credential), 30 giây ngân sách tải
remote/transaction. TCP/DNS connect có budget 3 giây; DNS hệ thống giữ giới hạn
concurrency chung. Các giới hạn này chưa phải tùy chọn TOML.

PEM/key pair được chuẩn bị trước commit. Fetch, TLS verification, parse PEM,
key mismatch hoặc bind/readiness lỗi giữ cấu hình đang chạy. Artifact được lưu
theo hash với quyền hạn chế; config commit tham chiếu bản local. Restart dùng
artifact đã commit, không cần nguồn online. GC giữ cả CA riêng của S3.

Thay nội dung file/object/URL không tự rotate cert. Gửi lại candidate có URI qua
`prostctl apply`/`reload` để lấy phiên bản mới. URL presign hết hạn sau commit
không ảnh hưởng bản đã tải; thời hạn certificate vẫn phải được quản lý riêng.
Với cặp cert/key trên S3, nên dùng prefix release bất biến hoặc versionId cho
từng object để tránh đọc lẫn hai lần phát hành. Validate và apply là hai lượt
độc lập; apply luôn kiểm tra lại.

`runtime.s3` là thiết lập nạp artifact, có thể đổi qua apply. Semantics chuyển
worker và drain: [rolling-updates.md](rolling-updates.md).
