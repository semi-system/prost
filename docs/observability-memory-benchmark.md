# Metrics và profiling RAM — 2026-11-13

## Điều kiện

Memory: 3 vòng mỗi điều kiện, thứ tự luân phiên; image/state mới. Mỗi run 10,000 HTTP/1 + 1,000 HTTPS/1 request 34 byte, 128 HTTP idle clients, 32 TLS readers giữ body 16 MiB, 10 apply weight dưới tải rồi cancel và chờ idle pool hết hạn.

Proxy 2 CPU/512 MiB, backend Go 2 CPU/256 MiB, 2 control threads + 2 I/O workers. Pool idle timeout 1 giây chỉ dùng trong harness. Mỗi stage có scrape metrics. Memory harness không ghim CPU và không kết luận throughput. Các container ngoài harness được giữ nguyên và ghi trong metadata.

Đo tốc độ tách riêng, proxy/backend 2 CPU/256 MiB, ghim guest cores client 0,2; proxy 4,6; backend 8,10. Không build hoặc chạy proxy đối thủ cùng lúc. Client/server cùng WSL2; Windows/hypervisor không được cô lập. Cache/log/buffering/retry tắt; byte/status/protocol được driver kiểm tra.

Image trước: `sha256:9b92619960e0545bd3053778488ac836b3e0753dcddf15349d18cf5964fd1d76`. Image cuối: `sha256:4b1e9ae90ceecb4391ea31f5c52981ac70543311376b5fe5227416d1da532d66`.

## RSS các stage không profiler

Trung vị MiB; các run giữ cùng binary mới cho hai cột sau, chỉ khác TLS buffer limit. Mặc định cuối vẫn là 64 KiB.

| Stage | Trước, 64 KiB | Mới mặc định, 64 KiB | Mới tùy chọn, 16 KiB |
|---|---:|---:|---:|
| Khởi động | 9.00 | 9.80 | 10.60 |
| Sau tải HTTP/HTTPS | 20.30 | 20.56 | 21.50 |
| 128 HTTP idle clients | 20.53 | 20.77 | 21.65 |
| 32 slow TLS streams | 28.19 | 28.14 | 27.23 |
| Sau 10 apply, stream còn sống | 28.09 | 28.34 | 27.45 |
| Sau cancellation | 23.80 | 24.08 | 25.24 |
| Sau pool expiry | 23.80 | 24.08 | 25.24 |

Cùng binary sau 10 apply: 16 KiB đổi RSS -3.2% so với 64 KiB; anonymous 19.49 → 18.24 MiB. RSS gồm file pages/stack/allocator, không phải heap live. Có dao động giữa các vòng; raw JSON ghi từng RSS và cgroup sample.

| Điều kiện | FD sau pool expiry | Generation còn sống ở mọi vòng |
|---|---:|---|
| Trước | 24 | [1, 1, 1] |
| Mới mặc định | 24 | [1, 1, 1] |
| Mới 16 KiB | 24 | [1, 1, 1] |

## Heaptrack riêng

Peak application heap từ massif: 17.84 → 15.91 MiB (-10.8%), so image cũ 64 KiB với image cuối tùy chọn 16 KiB. Các trace có cùng traffic/stages/scrapes; đây không phải kết quả RSS hay tốc độ của process không profiler.

Trace quy phần cấp phát lớn về BytesMut/Hyper HTTP/1 read buffers, rustls outbound payload và các future/connection. Giảm buffer TLS không loại bỏ phần Hyper read buffer; không giảm mặc định HTTP/1 max_buf_size vì nó cũng thay đổi giới hạn header upstream. Những allocation còn lại khi exit chưa đủ chứng minh leak. Không cộng các peak call stack đã merge để suy ra tổng heap.

## Header control

5 vòng, raw `/mnt/d/Windows/Desktop/code/Prost/test-results/metrics-header-final`. Cả hai image giữ TLS 64 KiB mặc định; so thay đổi metrics và các chỉnh sửa cuối.

| Case | req/s trước → sau | Throughput | p99 ms trước → sau | CPU µs/request trước → sau |
|---|---:|---:|---:|---:|
| native_header_34 | 39,735 → 39,394 | -0.9% | 7.72 → 8.35 | 42.11 → 42.62 |
| native_full_34 | 36,833 → 32,559 | -11.6% | 7.32 → 7.08 | 48.98 → 49.86 |

Mỗi image 1,000,000 request đo; tổng lỗi 0. Gate giảm throughput >5% hoặc tăng p99 >10%: native_full_34 cần điều tra.

## Body/stream control

3 vòng, raw `/mnt/d/Windows/Desktop/code/Prost/test-results/metrics-heavy-final`. Cả hai image giữ TLS 64 KiB mặc định; so thay đổi metrics và các chỉnh sửa cuối.

| Case | req/s trước → sau | Throughput | p99 ms trước → sau | CPU µs/request trước → sau |
|---|---:|---:|---:|---:|
| tls_h1_64k | 13,550 → 12,977 | -4.2% | 14.18 → 14.81 | 134.40 → 148.20 |
| h2_64k | 12,145 → 11,254 | -7.3% | 16.36 → 16.95 | 147.76 → 153.91 |
| double_tls_h2_64k | 9,865 → 9,725 | -1.4% | 21.63 → 18.57 | 177.57 → 182.29 |
| h2_stream_256k | 1,458 → 1,497 | +2.7% | 45.89 → 43.83 | 606.81 → 582.13 |
| grpc_stream | 1,709 → 1,704 | -0.3% | 45.98 → 48.75 | 968.80 → 937.14 |

Mỗi image 129,000 request đo; tổng lỗi 0. Gate giảm throughput >5% hoặc tăng p99 >10%: h2_64k cần điều tra.

## Đối chứng timed để điều tra gate

Raw `/mnt/d/Windows/Desktop/code/Prost/test-results/metrics-timed-control`: 5 vòng, mỗi ca 10 giây, cùng image/config/concurrency/CPU affinity. Mode này giữ các client hoạt động tới deadline; kết quả được giữ riêng với fixed-request.

| Case | req/s trước → sau | Throughput | p99 ms trước → sau | CPU µs/completed trước → sau |
|---|---:|---:|---:|---:|
| native_full_34 | 35,474 → 34,670 | -2.3% | 11.17 → 11.04 | 55.34 → 56.60 |
| h2_64k | 12,328 → 12,846 | +4.2% | 20.30 → 17.48 | 161.86 → 155.47 |

native_full_34: completed trước/sau 1,769,090/1,629,325; lỗi hoàn tất 0/0; unfinished tại deadline 640/640. CPU tính cả phần xử lý in-flight trước deadline; byte data cho phép phần body chưa hoàn tất trong giới hạn đã kiểm tra.


h2_64k: completed trước/sau 598,412/620,033; lỗi hoàn tất 0/0; unfinished tại deadline 640/640. CPU tính cả phần xử lý in-flight trước deadline; byte data cho phép phần body chưa hoàn tất trong giới hạn đã kiểm tra.

Gate timed: hai ca được điều tra đều nằm trong ngưỡng.

Các thread I/O trong raw timed chạy gần toàn bộ cửa sổ đo, giúp giảm ảnh hưởng phần đuôi chỉ một worker còn quota. Phân phối connection qua SO_REUSEPORT và quota client là một giả thuyết cho chênh lệch fixed-request lớn hơn chênh lệch CPU/request; chưa cô lập đầy đủ nguyên nhân bằng trace assignment. Timed không phủ nhận fixed-request: hai ca fixed vẫn giảm 7–12%, nên chưa tuyên bố không có regression trên mọi workload.

Control tốc độ ghi counters nhưng không scrape trong khoảng đo. Chi phí exposition chỉ được exercise trong test HTTP và các stage memory; chưa có benchmark throughput dưới nhiều scraper đồng thời.

## Các thử nghiệm đã giữ lại

Image thử `6f9c17bf6fca` mặc định TLS 16 KiB (`test-results/metrics-header-control`, `metrics-heavy-control`) giảm RAM nhưng header đầy đủ giảm throughput 7,1%; hai ca body 64 KiB giảm 13,3–15,4%. Không dùng cấu hình đó làm mặc định cuối. Đã giữ 64 KiB, thêm tùy chọn 16 KiB và tối ưu counter cho worker có một writer. Không quy mọi khác biệt giữa các lần chạy thành tác dụng của code; CPU/p99/throughput trên host này còn dao động.

Thử MALLOC_ARENA_MAX=2 có raw riêng `test-results/memory-arena2-control`, chỉ là một run khám phá; chưa dùng làm deployment default và không dùng để kết luận tiết kiệm RAM vì thiếu đối chứng CPU đầy đủ.

Raw memory: `/mnt/d/Windows/Desktop/code/Prost/test-results/memory-final`, heap trước/sau: `/mnt/d/Windows/Desktop/code/Prost/test-results/memory-heap-before-final`, `/mnt/d/Windows/Desktop/code/Prost/test-results/memory-heap-final`. Chưa soak 24 giờ hoặc định lượng allocator fragmentation bằng allocator-native counters. Các live pool gauges chỉ đếm pool groups được snapshot tham chiếu, không phải socket hay pool hit rate.

Cách tái hiện: [memory-profiling.md](memory-profiling.md). Định nghĩa metrics và nhãn: [observability.md](observability.md).
