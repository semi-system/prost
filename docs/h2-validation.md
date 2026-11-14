# Kiểm chứng latency H2 và soak edge

Ngày đo: 14/11/2026, Docker local trong WSL2; không chạy CI Runner. Lượt này
hoàn thiện công cụ đo và kiểm chứng, **không sửa data path Rust**, không đổi
cipher để lấy số đẹp và không tuyên bố đã giải quyết regression p99 trước đây.

## A/A trước khi kết luận A/B

Image dùng trong lượt đo này, trước khi bổ sung Runtime API rate table:
`sha256:89da44445826bb93bd459ed51b177369b510bae615f049a6373c5ec5dd4ef2b6`.
Máy Intel Core i7-10700, kernel `6.18.58.1-microsoft-standard-WSL2`.
Proxy/backend mỗi container 2 CPU / 256 MiB. Client ghim CPU 0,2; proxy 4,6;
backend 8,10, trên các core khác nhau theo topology guest. Hai I/O worker.
Affinity guest không dành riêng CPU của Windows. Các container ứng dụng khác
vẫn được giữ nguyên, danh sách lưu trong từng summary; không dừng chúng để đo.

Fixture `h2_64k`: frontend TLS/H2, backend HTTP/1.1, response 65.536 byte,
32 connection × 4 stream, h2load hai thread. Preflight soát byte/protocol;
mỗi interval 12 giây, không build hoặc soak song song. Config hai nhãn giống
nhau, SHA-256 `2016ac926e9e8983f21419407e99d6d994591bb60afebe9130ceba66914051a0`.

| A/A bão hòa, 5 lượt/nhãn | Nhãn before | Nhãn after |
|---|---:|---:|
| Binary/config | cùng image 89da… | cùng image 89da… |
| RPS median | 8.316,75 | 10.989,83 |
| p99 median | 35,455 ms | 21,360 ms |
| Khoảng RPS từng lượt | 6.599,67–12.981,17 | 7.880–12.493,50 |
| Khoảng p99 từng lượt | 15,111–51,301 ms | 16,005–39,335 ms |

Chênh lệch median **+32,14% RPS / −39,75% p99 trên cùng binary** vượt xa ngân
sách nhiễu 5%/10%. Tổng 1.172.469 request hoàn tất, 0 lỗi; 1.280 stream còn
dở tại deadline được báo riêng, không tính thành công. Tổng runnable wait của
hai worker có lượt lên 2.832 ms trong 12 giây; thời gian cgroup throttling
3,9–52,9 ms. Đây là bằng chứng scheduling có biến động, chưa đủ xác định toàn
bộ nguyên nhân; `schedstat` guest không phản ánh đầy đủ scheduling/frequency
của Windows. Không dùng lượt A/A này để khẳng định code nhanh hơn.

Raw: `test-results/h2-edge-aa/summary.json`, JSON/samples và TSV từng lượt.
Raw summary này được tạo trước khi bổ sung `gate.applicable_to_code_change`;
trường `calibration.outside_noise_budget=true` xác nhận A/A không đạt độ ổn định.

## Tải cố định dưới ngưỡng bão hòa

Thêm `--rps-per-client 100`: 3.200 request/giây tổng, giữ nguyên protocol,
payload, connection/stream và CPU. Đây là phép đo bổ sung, **không thay thế**
capacity workload cũ. Không có timed warmup bị tính nhầm CPU vào request chính.

A/A ba lượt/nhãn: 230.400 request hoàn tất, 0 lỗi/stream dở; đúng 3.200 RPS
từng lượt, không throttling. Median p99 hai nhãn 2,351 / 2,248 ms (−4,38%);
khoảng toàn bộ lượt 2,050–2,525 ms. Raw: `test-results/h2-edge-aa-paced`.

A/B với baseline gốc `prost:access-control`
(`sha256:b39b808f5376130bed52ad5ee1b525fe3ea76cd29bab7358976cd5856bf09316`)
và 89da…, ba lượt/nhãn, cùng config/tải cố định:

| Chỉ số | Baseline | Hiện tại |
|---|---:|---:|
| RPS median | 3.200 | 3.199,92 |
| p99 từng lượt (ms) | 2,124 / 5,168 / 5,378 | 2,179 / 2,388 / 2,919 |
| p99 median (ms) | 5,168 | 2,388 |
| CPU median (µs/request) | 203,79 | 182,23 |
| Lỗi / stream dở ở deadline | 0 / 3 | 0 / 0 |

Raw: `test-results/h2-edge-paced-ab`. Median thuận lợi trong phép đo này
không chứng minh cải thiện p99 54% do code: các lượt baseline chênh trên hai
lần, môi trường thay đổi theo thời điểm. Regression bão hòa lịch sử vẫn mở.
Đối chứng A/A sau A/B được lưu riêng ở `test-results/h2-edge-aa-paced-after`:
median p99 2,310 / 2,384 ms (+3,20%); khoảng từng lượt 1,841–2,594 ms,
230.376 request hoàn tất/0 lỗi và 11 stream dở tại deadline. Không throttling.

## CPU profile và sửa công cụ

Profile riêng 12 giây, `cpu-clock:u` 199 Hz: 2.150 sample, không lost sample.
TLS 1.3, `TLS_AES_128_GCM_SHA256`. AES-GCM `_aesni_ctr32_ghash_6x` khoảng
19,30% self sample; `h2::...Connection::poll` 1,58%, Hyper upstream send_request
1,26%, Prost handle_http 0,84%. Self sample ngắn không phải tỷ lệ toàn bộ
request latency hoặc bằng chứng nguyên nhân p99. Throughput khi profiling
không được dùng làm benchmark. Raw: `test-results/h2-edge-profile`.

Sửa `symbolize-http-cpu.py`: giữ cả tên function đã được perf resolve và địa chỉ
hex cần resolve bằng ELF của đúng image; trước đây chỉ nhận hex nên có thể tạo
danh sách function rỗng khi perf đã có tên. Số phần trăm là tổng các hàng đã
làm tròn, có thể không đúng 100%. Không phục hồi được đầy đủ inline call stack.

Resource sampler đọc thêm cgroup CPU periods/throttling. Benchmark lưu worker
run/wait/timeslice, host CPU pressure, cpuset/cpu.max và p99/CPU từng lượt.
Thread biến mất giữa các lần đọc procfs không tạo sample dở. A/A được gắn rõ
không áp dụng gate thay đổi code; đánh dấu nhiễu theo cả hai hướng. Các thư mục
đã có kết quả không bị ghi đè. Sáu test parser/profiler đạt.

Các thử nghiệm tối ưu tiếp theo cần profile/latency trace trên host ổn định:
TLS write batching, chi phí allocation và scheduling/queue của stream H2.
Chưa thay đổi kiến trúc worker hoặc giảm bảo đảm timeout/backpressure khi chưa
có bằng chứng. Hướng dẫn chạy: [benchmarks/README.md](../benchmarks/README.md).

## Soak đúng image và policy

Lượt cũ `test-results/soak-24h-v2` dùng image a7e9…, đã **dừng chủ đích** để
tách benchmark: checkpoint cuối 3.874 giây, 2.031.168 H1 request/0 lỗi,
36.300 H2 probe, 363 hot apply. `controlled-stop.json` ghi nguyên nhân;
progress giữ trạng thái interrupted/failed, không đổi thành passed 24 giờ.

Soak mới có `--edge-policies`: Host ACL trên H1/H2, keepalive idle 30 giây
với header budget 1 giây, TCP dial retry và connection window; probe Host lạ
421 sau mỗi apply, kiểm tra reuse sau idle 1,5 giây. Giữ workload hỗn hợp,
compression/cache, TLS/H2, TCP giữ generation cũ, log rotate và artifact GC.
Healthy soak không chứng minh nhánh retry recovery hoặc rate rejection;
integration test riêng kiểm tra các nhánh đó.

Smoke 30 giây của image 89da… đạt: 15.353 H1/0 lỗi, 300 H2, 3 hot apply,
4 TCP probe, 4 Host denial đúng 421, 1 idle reuse; RSS đỉnh 21,43 MiB,
85 FD, tối đa 2 generation, cuối tải 0 connection/1 generation.
Raw: `test-results/edge-soak-check/summary.json`. Đây không phải capacity test.

`scripts/start-soak.py` khởi chạy nền bằng immutable ID cho cả proxy/backend,
kiểm tra certificate đủ thời hạn, ghi PID/command/hash script/giờ kết thúc dự
kiến. Chỉ `summary.json` hoàn tất mới chứng minh soak đạt; launcher thành công
hoặc progress đang chạy không tương đương đã qua 24 giờ.

Lượt mới `test-results/soak-24h-edge` khởi chạy **14:35 ngày 14/11/2026**
(Asia/Saigon), dự kiến kết thúc khoảng **14:35 ngày 15/11/2026** nếu máy/WSL
tiếp tục hoạt động. Proxy là image 89da… ở trên; backend khóa ở
`sha256:9be01d5618bd191296a2f3235d380406b50d49452791f023e4456d1f27cee206`.
Trạng thái tại lúc viết: đang chạy, chưa có kết luận đạt 24 giờ. Theo dõi
`progress.json`, `samples.jsonl` và `run.log` trong thư mục đó.
Launcher đã được kiểm tra riêng qua một chiến dịch ngắn hoàn tất:
`test-results/edge-launcher-check` (4.824 H1/0 lỗi, 200 H2, 2 apply).
