"""Render a reviewable Markdown report from the heavy benchmark's saved measurements."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LABELS = {
    "h1_1m": "HTTP/1 download 1 MiB",
    "tls_h1_64k": "HTTPS/1 download 64 KiB",
    "h2_64k": "HTTPS/2 download 64 KiB",
    "double_tls_h2_64k": "TLS hai phía, upstream HTTP/1",
    "h2_upload_1m": "HTTPS/2 upload 1 MiB + SHA-256",
    "h2_stream_256k": "HTTPS/2 stream 256 KiB",
    "grpc_stream": "gRPC framed stream + trailers",
    "route1000_64k": "1.000 route, body 64 KiB",
    "native_header_64k": "Header tĩnh, body 64 KiB",
    "native_header_34": "Header tĩnh, body 34 byte",
    "native_chain_34": "ACL + rate limit + header",
    "native_full_34": "Header động + request ID + expires",
    "native_trailer_64k": "Native trailer, body 64 KiB",
    "native_map_34": "Map exact + condition + expires",
    "native_map_regex_34": "Map regex + condition + expires",
    "native_keyed_rate_34": "Rate limit theo header, một hot key",
    "native_rewrite_exact_34": "Rewrite exact → /bytes/34",
    "native_rewrite_prefix_34": "Rewrite prefix → /bytes/34",
    "native_rewrite_regex_34": "Rewrite regex + capture → /bytes/34",
}


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def number(value, digits=0):
    return f"{value:,.{digits}f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--title", default="Benchmark native header và workload nặng — 2026-11-11")
    parser.add_argument("--image-control", type=Path, help="Alternating header-image control summary")
    parser.add_argument("--previous-image-control", type=Path, help="Earlier investigation summary to preserve regression evidence")
    parser.add_argument("--slow-readers", type=Path)
    parser.add_argument("--live-updates", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT/"docs/native-benchmark.md")
    args = parser.parse_args()
    summary = read(args.directory/"summary.json")
    rows = summary["results"]
    meta = summary["metadata"]
    requests = sum(row["prost"]["requests"] for row in rows.values())
    errors = {target: sum(row[target]["failures"] for row in rows.values()) for target in ("prost", "nginx")}
    lines = [f"# {args.title}", "",
             f"Mỗi proxy hoàn tất **{number(requests)} request đo** trên {len(rows)} workload, "
             f"{meta['rounds']} vòng. Lỗi: Prost {errors['prost']}, nginx {errors['nginx']}.", "",
             f"Raw: `{args.directory.relative_to(ROOT) if args.directory.is_relative_to(ROOT) else args.directory}/`. "
             "Bảng dùng trung vị các vòng; RAM là đỉnh working set lấy mẫu lớn nhất.", "",
             "## Cùng điều kiện", "",
             f"- CPU: {meta['cpu']}; kernel `{meta['kernel']}`; Docker trong WSL2.",
             "- Mỗi proxy và backend: quota 2 CPU, giới hạn 256 MiB. Release portable, thin LTO, không PGO.",
             f"- Phiên bản: `{meta['versions']['prost']}`, `{meta['versions']['nginx']}`; driver `{meta['h2load']}`.",
             "- Cùng backend Go, payload, certificate, client concurrency và timeout; không access log hoặc response cache.",
             "- nginx: 2 worker, reuseport, buffer streaming 64 KiB, body preread HTTP/2 1 MiB; pool HTTP/HTTPS/gRPC riêng, 128 idle tổng mỗi pool. Buffering và retry tắt.",
             "- Prost: 2 I/O worker, HTTP/1 upstream buffer tối đa 64 KiB, pool idle có timer 30 giây. WASM tắt khi build.",
             "- Phần lớn HTTP/2 dùng 32 connection × 4 stream; upload/delayed stream dùng 16 × 4. Thứ tự proxy và workload luân phiên.",
             "- Warmup 256 request trước mỗi phép đo. Log request và file upload đặt trên filesystem Linux; không build/profile khi đo.",
             "- Preflight kiểm tra protocol upstream, byte body, hash upload, header động/expires và native/gRPC trailers. Khi đo kiểm tra status 200, số response, số mẫu latency và tổng byte. Backend kiểm tra hash mỗi upload.", "",
             "Expires/Cache-Control trong ca header động là header cho client. Driver không cache và cả hai proxy đều forward request tới backend.", "",
             "## Throughput và latency", "",
             "| Workload | Prost req/s | nginx req/s | Chênh lệch Prost | p99 Prost / nginx (ms) |",
             "|---|---:|---:|---:|---:|"]
    for case, row in rows.items():
        p, n = row["prost"], row["nginx"]
        difference = (p["rps_median"]/n["rps_median"]-1)*100
        lines.append(f"| {LABELS.get(case,case)} | {number(p['rps_median'])} | {number(n['rps_median'])} | {difference:+.1f}% | {number(p['p99_ms_median'],2)} / {number(n['p99_ms_median'],2)} |")
    lines += ["", "## CPU và bộ nhớ", "",
              "| Workload | CPU µs/request Prost / nginx | Working set MiB Prost / nginx |",
              "|---|---:|---:|"]
    for case, row in rows.items():
        p, n = row["prost"], row["nginx"]
        lines.append(f"| {LABELS.get(case,case)} | {number(p['cpu_us_per_request_median'],2)} / {number(n['cpu_us_per_request_median'],2)} | {number(p['working_set_peak_mib'],2)} / {number(n['working_set_peak_mib'],2)} |")
    more_ram = sum(row["prost"]["working_set_peak_mib"] > row["nginx"]["working_set_peak_mib"] for row in rows.values())
    lines += ["", f"Prost dùng working set đỉnh cao hơn nginx ở {more_ram}/{len(rows)} workload. "
              "Đây là số đo cgroup (`memory.current - inactive_file`), không phải heap/RSS riêng. Lấy mẫu mỗi 50 ms có thể bỏ lỡ spike. "
              "Mỗi proxy giữ cùng process qua nhiều workload trong một vòng, nên cache/buffer còn lại từ ca trước ảnh hưởng RAM.", ""]
    if "native_map_34" in rows:
        lines += ["## Phạm vi policy workload", "",
                  "Các ca trong báo cáo này dùng body 34 byte trên HTTPS/2, 32 connection × 4 stream. Map exact và regex nhận Origin khác nhau để chọn đúng nhánh, rewrite request header, kiểm tra giá trị backend echo, thêm response header có điều kiện và expires động. nginx dùng native map/headers để tạo cùng kết quả.", "",
                  "Ca rate limit theo header dùng một API key không rỗng cho toàn bộ request, rate và burst rất cao để đo chi phí admission khi không bị từ chối. Không đo cardinality lớn hoặc throughput của HTTP 429. Isolation, capacity, idle eviction và quota qua hot update được kiểm tra riêng bằng test chức năng.", "",
                  "Working set khoảng 25 MiB trong bộ tải nhỏ này không thay thế số đo body lớn/slow client. Xem [báo cáo workload nặng](native-benchmark.md) cho các giới hạn bộ nhớ và hiệu năng còn lại. Các báo cáo có source hash và image ID khác nhau; không dùng chênh lệch giữa hai lần chạy làm bằng chứng tối ưu RAM.", ""]
    if "native_rewrite_exact_34" in rows:
        lines += ["## Phạm vi rewrite workload", "",
                  "Cả ba ca rewrite dùng HTTPS/2, body 34 byte, 32 connection × 4 stream và forward tới /bytes/34. nginx dùng proxy_pass có URI tĩnh/thay prefix cho hai ca đầu, và native rewrite + capture PCRE cho ca regex. Prost dùng plugin rewrite native với matcher tương ứng. Hai ca header là control cùng lần chạy; không có thay đổi URI trong hai ca đó.", "",
                  "Direct-backend control gọi /bytes/34 trực tiếp. Nếu proxy bỏ qua rewrite, fixture nhận path gốc sẽ trả body kích thước khác và preflight sẽ fail. Không benchmark redirect ở đây vì nginx mặc định tạo body HTML trong khi Prost trả body rỗng; status/Location, query, ACL/rate ordering và streaming được kiểm tra bằng test chức năng.", "",
                  "Số đo này không đại diện body lớn, nhiều client chậm hoặc sức chứa tối đa TLS handshake/SNI. Xem [báo cáo workload nặng](native-benchmark.md) và [hành vi rewrite](rewrite.md).", ""]
    if args.image_control:
        control = read(args.image_control)
        previous = read(args.previous_image_control) if args.previous_image_control else None
        control_rounds = control.get("rounds", 3)
        control_requests = sum(row["before"]["requests"] for row in control["results"].values())
        per_round = next(iter(control["results"].values()))["before"]["requests"] // control_rounds
        lines += ["## Đối chứng đường xử lý không dùng rewrite", "",
                  f"Đã kiểm tra image trước thay đổi và image hiện tại trên cùng config, luân phiên thứ tự image và thứ tự workload. Mỗi ca/vòng {number(per_round)} request, {control_rounds} vòng, {number(control_requests)} request mỗi image, không build hoặc chạy nginx đồng thời. Các plugin rewrite trong fixture được thay bằng header filter rỗng cho cả hai image để bản cũ đọc được, giữ nguyên số route. Đây là phép đo riêng với bảng nginx ở trên.", "",
                  "Bản đầu đã vượt ngưỡng regression, vì vậy đã tách xử lý URI sang future riêng chỉ cấp phát khi dùng, giữ metadata URI mới ngoài context mặc định và dùng Arc cho dữ liệu redirect. Sau thay đổi chạy lại cả test/smoke và phép đối chứng.", "",
                  "| Ca | Chênh throughput bản đầu | Throughput trước → hiện tại | Chênh hiện tại | p99 trước → hiện tại (ms) |",
                  "|---|---:|---:|---:|---:|"]
        for case, row in control["results"].items():
            before, after = row["before"], row["after"]
            delta = (after["rps"]/before["rps"]-1)*100
            early = previous["results"].get(case) if previous else None
            early_delta = f"{(early['after']['rps']/early['before']['rps']-1)*100:+.1f}%" if early else "—"
            lines.append(f"| {LABELS.get(case,case)} | {early_delta} | {number(before['rps'])} → {number(after['rps'])} | {delta:+.1f}% | {number(before['p99_ms'],2)} → {number(after['p99_ms'],2)} |")
        first = next(iter(control["results"].values()))
        exceeds = sum(row["after"]["rps"] < row["before"]["rps"] * .95 or row["after"]["p99_ms"] > row["before"]["p99_ms"] * 1.1 for row in control["results"].values())
        gate_note = "Không có ca vượt ngưỡng" if not exceeds else f"Có {exceeds} ca vượt ngưỡng, cần điều tra"
        lines += ["", f"Image trước: `{first['before']['image_id']}`. Image hiện tại: `{first['after']['image_id']}`.", "",
                  f"Raw đối chứng: `{args.image_control.parent}`. {gate_note} throughput giảm 5% hoặc p99 tăng 10% trong phép đo hiện tại. Máy dùng chung vẫn có dao động giữa các vòng; không quy toàn bộ chênh lệch throughput thành lợi ích CPU của một thay đổi cụ thể.", ""]
    for path, title in [(args.slow_readers,"Client đọc chậm, cùng workload"),(args.live_updates,"Hot update khi stream đang chạy")]:
        if path:
            data = read(path)
            lines += [f"## {title}", "", "| Proxy | Clients / thời gian | Working set đỉnh MiB | Kết quả |", "|---|---:|---:|---|"]
            for r in data:
                state = r.get("final_status")
                result = "Không lỗi, không OOM"
                if state:
                    result += f"; {state['connections']} connection, {state['live_generations']} generation sau cleanup"
                lines.append(f"| {r['target']} | {r['clients']} / {r['duration_seconds']}s | {number(r['working_set_peak_mib'],2)} | {result} |")
            if path == args.live_updates:
                lines += ["", "Prost disable/enable endpoint trong lúc stream cũ tiếp tục đọc; request mới lần lượt nhận 503/200. nginx chạy cùng slow-reader workload nhưng không đổi config, nên RAM của phép thử này không phải so sánh thao tác cập nhật tương đương."]
            lines += [""]
    lines += ["## Phạm vi kết luận", "",
              "- Đây là workload local trên máy dùng chung, không CPU pinning hoặc load generator riêng; không suy rộng thành mọi workload/production capacity.",
              "- gRPC dùng fixture byte-framing và grpc-status, không có chi phí protobuf/business handler. Hai proxy đều xác minh TLS upstream và dùng keep-alive; kiến trúc multiplex/pool vẫn khác nhau.",
              "- Direct-backend control nằm trong raw. Protocol của direct client có thể khác protocol upstream sau proxy, nên không dùng direct RPS làm trần tuyệt đối hoặc trừ để suy ra overhead.",
              "- Các thư mục đo trước `native-verified-final` là exploratory; đã phát hiện và sửa ALPN cùng cấu hình keep-alive gRPC. Không dùng chúng để kết luận so sánh cuối.",
              "", f"Source SHA-256: `{meta['source_sha256']}`. Image ID, limits, config hash và toàn bộ mẫu đo có trong JSON từng vòng.",
              "", "Lệnh tái hiện: [benchmarks/README.md](../benchmarks/README.md). "
              "Hành vi và giới hạn tính năng: [native-headers.md](native-headers.md).", ""]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if "grpc_stream" not in rows:
        lines = [line for line in lines if not line.startswith("- gRPC dùng fixture")]
    args.output.write_text("\n".join(lines), encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
