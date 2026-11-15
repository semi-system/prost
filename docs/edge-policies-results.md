# Kiểm chứng HTTP/TCP edge policy

Mục tiêu là hoàn thiện Prost độc lập với ứng dụng/backend cụ thể. Hướng dẫn:
[edge-policies.md](edge-policies.md); ví dụ chung [edge.toml](../examples/edge.toml).
Không thực hiện chuyển đổi toàn bộ cấu hình mail service.

## Build và test

Build release native bằng Docker trong WSL, Rust 1.93, Cargo.lock, thin LTO,
WASM tắt. Image:
`sha256:89da44445826bb93bd459ed51b177369b510bae615f049a6373c5ec5dd4ef2b6`.
Image đối chứng trước thay đổi vẫn giữ ở prost:edge-control (a7e9c3e38f7d).

Format check, clippy all-targets/all-features với -D warnings, toàn bộ native và
all-features: **154 test đạt ở mỗi chế độ**, harness bốn test thread, hai build
job. Một helper crash test được test cha gọi qua subprocess. Raw:
`test-results/edge-final-checks-v2.log`. Test HTTPS bổ sung sau khi build chỉ đổi
test source, không đổi code binary release.

Các ca mới kiểm tra:

- Host/authority, IPv6 host, case/port normalization, missing/duplicate Host,
  authority conflict, userinfo và nonnumeric port; Host ACL chặn trước redirect,
  H1/H2 và hot update trên kết nối hiện có.
- Idle budget dài hơn header budget, đóng rảnh không gửi HTTP 408, không tăng
  error counter; HTTPS tái sử dụng và TLS close sạch; streaming/H2 không bị cắt.
- Prefix request tiếp theo đã pipeline trong buffer vẫn dùng header deadline;
  scanner nhận input từng byte, Content-Length/chunk extensions/trailers, scratch
  không vượt 32 KiB; write backpressure trì hoãn idle expiry.
- TCP dial retry phục hồi rồi relay đúng payload một lần; giữ endpoint healthy
  khi phục hồi; giới hạn attempts/total budget, không retry lỗi sau PROXY/TLS.
- Weighted window giữ state qua apply/đa worker, tính rejected attempt, qua ranh
  giới period/expiry/quota; timestamp đến shard lock không đúng thứ tự không gây
  underflow hoặc reset counter.

Ca malformed Host phát hiện http::Authority có thể giữ suffix không phải số
trong khi port() trả None. Đã sửa kiểm tra suffix và chạy lại; không bỏ ca đó
hoặc chấp nhận hostname không đúng allowlist để làm test qua.

## Release workload 10.000 request

Chạy `scripts/edge-smoke.py` với proxy và backend riêng, mỗi container giới hạn
2 CPU / 256 MiB, proxy hai I/O worker. 32 H1 keepalive client, 5.000 response
34 byte và 5.000 response 64 KiB, body được soát byte. Native Host ACL, response
headers, keepalive idle clock và connection window đều bật.

Trong tải có 10 hot apply, kiểm tra header generation trên keepalive và một TCP
session giữ snapshot cũ qua toàn bộ update. TCP backend là HTTP fixture thông
thường, không phụ thuộc ứng dụng mail. Kiểm tra thêm 421/308/Location, reuse sau
800 ms khi header budget 500 ms và idle budget 2000 ms; 1.000 H2 probe sau tải.

| Kết quả | Giá trị |
|---|---|
| Request chính / lỗi | 10.000 / 0 |
| Hot apply / TCP probe | 10 / 11 |
| H2 probe thành công | 1.000 / 1.000 |
| CPU proxy/request | 169,95 µs, gồm hot apply/control probe trong interval |
| Process RSS đỉnh lấy mẫu | 15,18 MiB |
| Cgroup working set đỉnh lấy mẫu | 14,29 MiB |
| FD đỉnh lấy mẫu | 94 |
| Sau đóng tải | 0 connection, 1 live generation |

Workload có pause 2 ms/request để xen kẽ update; **không phải capacity benchmark**.
Lượt soak image cũ chạy riêng trong lúc kiểm thử, nên không so RPS/p99 với image
cũ hoặc Nginx từ bài này. Raw: `test-results/edge-final-smoke/summary.json`,
samples.json và h2load.txt. Demo HTTP/TCP/native policy/balancing/backup/passive
health/atomic apply cũng đạt; bốn test parser benchmark đạt.

Tái hiện sau khi build backend fixture:

```sh
sudo docker build -t prost:bundle-backend benchmarks/advanced
sudo python3 scripts/edge-smoke.py --image prost:native \
  --output-dir test-results/edge-smoke-new
```

## Giới hạn kết luận

Các policy mới opt-in. Weighted window là xấp xỉ. Image 89da… trong báo cáo này
chưa có API liệt kê/xóa rate table; phiên bản sau đã bổ sung:
[rate-tables.md](rate-tables.md). TCP retry cùng endpoint, chỉ ở dial, không phải failover/replay.
Header clock chụp settings khi accept; HTTP/2 không dùng timeout idle HTTP/1.
Ví dụ TLS cần certificate/SNI/backend thực tế; `prost check` chỉ kiểm tra schema.

Soak 24 giờ ở `test-results/soak-24h-v2` dùng **image cũ a7e9…**, không chứng minh
image 89da… đã qua 24 giờ; lượt đó đã dừng chủ đích sau khoảng 65 phút để tách
benchmark, có `controlled-stop.json` giải thích. Kiểm chứng mới bật edge policy,
A/A noise, CPU profile và launcher soak theo immutable image:
[h2-validation.md](h2-validation.md).
H2 p99 regression trong các đối chứng bão hòa trước vẫn chưa được giải quyết;
không tuyên bố nhanh hơn Nginx hoặc sẵn sàng production từ các test này.
