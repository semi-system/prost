# Các phần chức năng tiếp theo

Ưu tiên hiện tại là hoàn thiện chức năng và kiểm thử local Docker/WSL.
Tối ưu throughput/p99 và PGO được để sau; các báo cáo benchmark cũ giữ nguyên
kết luận và giới hạn đã ghi, không dùng để xác nhận chức năng mới.

| Phần | Trạng thái | Công việc tiếp theo |
|---|---|---|
| TLS passthrough chọn upstream theo SNI | Đã bổ sung | Exact/wildcard, strict/default, PROXY/mTLS, hot apply và restart đã test |
| Quản trị native rate table | Đã bổ sung | list/show/clear với generation, giữ phiên đang chạy |
| Routing HTTP theo method/header | Đã bổ sung | Method list, literal/prefix/regex/presence, thứ tự match và hot update |
| DNS backend có TTL | Đã bổ sung | TTL cấu hình, negative cache, singleflight, nhiều IP, health và đổi pool khi tập IP đổi |
| Quản trị/invalidation cache | Đã bổ sung | Status/entries/key, purge exact/all theo upstream, epoch chặn fill cũ và giữ memory lease |
| HTTP cache nâng cao | Đã bổ sung revalidation/stale | ETag/Last-Modified/304, stale-if-error có giới hạn, giữ purge barrier; general Vary còn thiếu |
| Rolling worker generation | Đã bổ sung | Prepare/readiness trước commit, clone socket, drain có deadline, tối đa hai thế hệ |
| Chờ và quan sát rolling | Đã bổ sung | Runtime API `reload.wait`, CLI `reload --wait`/`workers wait`, connection/task/deadline và metrics |
| TLS artifact URI/S3 | Đã bổ sung | File/HTTP(S)/SigV4 S3, versionId, staging và restart offline từ hash artifact |

Routing hiện tại kết hợp host/path prefix/method/header với priority. Native
header/map/rewrite tiếp tục là filter sau khi đã chọn route.
DNS dùng resolver hệ thống với TTL cache tùy cấu hình; chưa đọc authoritative
DNS TTL hoặc SRV record. Cache có conditional revalidation và stale-on-error
opt-in; vẫn bypass Vary và dữ liệu không phù hợp. Cache disk và
stale-while-revalidate chưa triển khai.

Các mục này không có nghĩa phải triển khai mọi directive/module của Nginx mới
chạy được MVP. Mỗi phần cần có schema rõ, hot apply/persistence tương ứng và
test lỗi/streaming để tránh làm hỏng các giao thức đang chạy.

UDP, HTTP/3, STARTTLS offload và control plane phân tán nằm ngoài v1 ban đầu;
cache disk chưa được đưa vào đợt hoàn thiện này. TLS passthrough có SNI chỉ xử lý TLS ngay từ đầu kết nối;
không phải STARTTLS. Các phần authentication hoặc session affinity bằng cookie
có thể chốt phạm vi riêng khi cần.

Hướng dẫn: [passthrough-routing.md](passthrough-routing.md),
[http-routing.md](http-routing.md), [rate-tables.md](rate-tables.md),
[cache-control.md](cache-control.md), [dns-cache.md](dns-cache.md). Production qualification vẫn cần soak đủ thời
gian trên đúng image và kiểm chứng vận hành cho workload triển khai thực tế.
