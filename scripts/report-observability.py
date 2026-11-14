"""Generate observability/RAM report from saved measurements; sends no traffic."""
import argparse
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent.parent
STAGES = {"startup": "Khởi động", "after_load": "Sau tải HTTP/HTTPS", "idle_clients": "128 HTTP idle clients",
          "slow_tls_streams": "32 slow TLS streams", "after_10_updates": "Sau 10 apply, stream còn sống",
          "after_cancel": "Sau cancellation", "after_pool_expiry": "Sau pool expiry"}


def read(path): return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--memory", type=Path, default=ROOT / "test-results/memory-final")
    parser.add_argument("--header", type=Path, default=ROOT / "test-results/metrics-header-final")
    parser.add_argument("--heavy", type=Path, default=ROOT / "test-results/metrics-heavy-final")
    parser.add_argument("--timed", type=Path, default=ROOT / "test-results/metrics-timed-control")
    parser.add_argument("--heap-before", type=Path, default=ROOT / "test-results/memory-heap-before-final")
    parser.add_argument("--heap-after", type=Path, default=ROOT / "test-results/memory-heap-final")
    parser.add_argument("--output", type=Path, default=ROOT / "docs/observability-memory-benchmark.md")
    args = parser.parse_args()
    memory = read(args.memory / "summary.json")
    before, after, compact = [memory["results"][key] for key in ("before", "after", "after_16k")]
    profile = read(args.memory / "before-1/summary.json")["metadata"]
    n = profile["requests"]
    lines = ["# Metrics và profiling RAM — 2026-11-13", "",
        "## Điều kiện", "",
        f"Memory: {memory['rounds']} vòng mỗi điều kiện, thứ tự luân phiên; image/state mới. Mỗi run {n:,} HTTP/1 + {max(256,n//10):,} HTTPS/1 request 34 byte, {profile['idle_clients']} HTTP idle clients, {profile['slow_tls_streams']} TLS readers giữ body 16 MiB, 10 apply weight dưới tải rồi cancel và chờ idle pool hết hạn.", "",
        "Proxy 2 CPU/512 MiB, backend Go 2 CPU/256 MiB, 2 control threads + 2 I/O workers. Pool idle timeout 1 giây chỉ dùng trong harness. Mỗi stage có scrape metrics. Memory harness không ghim CPU và không kết luận throughput. Các container ngoài harness được giữ nguyên và ghi trong metadata.", "",
        "Đo tốc độ tách riêng, proxy/backend 2 CPU/256 MiB, ghim guest cores client 0,2; proxy 4,6; backend 8,10. Không build hoặc chạy proxy đối thủ cùng lúc. Client/server cùng WSL2; Windows/hypervisor không được cô lập. Cache/log/buffering/retry tắt; byte/status/protocol được driver kiểm tra.", "",
        f"Image trước: `{before['image_id']}`. Image cuối: `{after['image_id']}`.", "",
        "## RSS các stage không profiler", "",
        "Trung vị MiB; các run giữ cùng binary mới cho hai cột sau, chỉ khác TLS buffer limit. Mặc định cuối vẫn là 64 KiB.", "",
        "| Stage | Trước, 64 KiB | Mới mặc định, 64 KiB | Mới tùy chọn, 16 KiB |", "|---|---:|---:|---:|"]
    for key, label in STAGES.items():
        a,b,c = [group["stages"][key] for group in (before,after,compact)]
        lines.append(f"| {label} | {a['rss_mib']:.2f} | {b['rss_mib']:.2f} | {c['rss_mib']:.2f} |")
    key = "after_10_updates"
    b,c = [group["stages"][key] for group in (after,compact)]
    lines += ["", f"Cùng binary sau 10 apply: 16 KiB đổi RSS {(c['rss_mib']/b['rss_mib']-1)*100:+.1f}% so với 64 KiB; anonymous {b['anonymous_mib']:.2f} → {c['anonymous_mib']:.2f} MiB. RSS gồm file pages/stack/allocator, không phải heap live. Có dao động giữa các vòng; raw JSON ghi từng RSS và cgroup sample.", "",
        "| Điều kiện | FD sau pool expiry | Generation còn sống ở mọi vòng |", "|---|---:|---|"]
    for label, group in [("Trước",before),("Mới mặc định",after),("Mới 16 KiB",compact)]:
        stage = group["stages"]["after_pool_expiry"]
        lines.append(f"| {label} | {stage['fds']:.0f} | {stage['live_generations']} |")
    heaps = []
    for directory in (args.heap_before,args.heap_after):
        text = (directory / "massif.txt").read_text()
        heaps.append(max(map(int,re.findall(r"^mem_heap_B=(\d+)$",text,re.M))))
    lines += ["", "## Heaptrack riêng", "",
        f"Peak application heap từ massif: {heaps[0]/1048576:.2f} → {heaps[1]/1048576:.2f} MiB ({(heaps[1]/heaps[0]-1)*100:+.1f}%), so image cũ 64 KiB với image cuối tùy chọn 16 KiB. Các trace có cùng traffic/stages/scrapes; đây không phải kết quả RSS hay tốc độ của process không profiler.", "",
        "Trace quy phần cấp phát lớn về BytesMut/Hyper HTTP/1 read buffers, rustls outbound payload và các future/connection. Giảm buffer TLS không loại bỏ phần Hyper read buffer; không giảm mặc định HTTP/1 max_buf_size vì nó cũng thay đổi giới hạn header upstream. Những allocation còn lại khi exit chưa đủ chứng minh leak. Không cộng các peak call stack đã merge để suy ra tổng heap.", ""]
    for title, directory in [("Header control", args.header),("Body/stream control", args.heavy)]:
        data = read(directory / "summary.json")
        lines += [f"## {title}", "", f"{data['rounds']} vòng, raw `{directory}`. Cả hai image giữ TLS 64 KiB mặc định; so thay đổi metrics và các chỉnh sửa cuối.", "",
            "| Case | req/s trước → sau | Throughput | p99 ms trước → sau | CPU µs/request trước → sau |", "|---|---:|---:|---:|---:|"]
        failures = 0; requests = 0; gates = []
        for case,row in data["results"].items():
            a,b = row["before"],row["after"]
            failures += a["failures"]+b["failures"]; requests += a["requests"]
            if row["gate"]["investigation_required"]: gates.append(case)
            lines.append(f"| {case} | {a['rps']:,.0f} → {b['rps']:,.0f} | {row['gate']['throughput_delta_percent']:+.1f}% | {a['p99_ms']:.2f} → {b['p99_ms']:.2f} | {a['cpu_us']:.2f} → {b['cpu_us']:.2f} |")
        lines += ["", f"Mỗi image {requests:,} request đo; tổng lỗi {failures}. Gate giảm throughput >5% hoặc tăng p99 >10%: "+(", ".join(gates)+" cần điều tra." if gates else "không ca vượt ngưỡng trong phép đo này."), ""]
    timed = read(args.timed / "summary.json")
    lines += ["## Đối chứng timed để điều tra gate", "",
        f"Raw `{args.timed}`: {timed['rounds']} vòng, mỗi ca {timed['duration_seconds']} giây, cùng image/config/concurrency/CPU affinity. Mode này giữ các client hoạt động tới deadline; kết quả được giữ riêng với fixed-request.", "",
        "| Case | req/s trước → sau | Throughput | p99 ms trước → sau | CPU µs/completed trước → sau |", "|---|---:|---:|---:|---:|"]
    timed_gates = []; timed_notes = []
    for case,row in timed["results"].items():
        a,b = row["before"],row["after"]
        if row["gate"]["investigation_required"]: timed_gates.append(case)
        lines.append(f"| {case} | {a['rps']:,.0f} → {b['rps']:,.0f} | {row['gate']['throughput_delta_percent']:+.1f}% | {a['p99_ms']:.2f} → {b['p99_ms']:.2f} | {a['cpu_us']:.2f} → {b['cpu_us']:.2f} |")
        timed_notes += ["",f"{case}: completed trước/sau {a['requests']:,}/{b['requests']:,}; lỗi hoàn tất {a['failures']}/{b['failures']}; unfinished tại deadline {a['unfinished_at_deadline']}/{b['unfinished_at_deadline']}. CPU tính cả phần xử lý in-flight trước deadline; byte data cho phép phần body chưa hoàn tất trong giới hạn đã kiểm tra.",""]
    lines += timed_notes + ["Gate timed: "+(", ".join(timed_gates)+" vượt ngưỡng." if timed_gates else "hai ca được điều tra đều nằm trong ngưỡng."),"",
        "Các thread I/O trong raw timed chạy gần toàn bộ cửa sổ đo, giúp giảm ảnh hưởng phần đuôi chỉ một worker còn quota. Phân phối connection qua SO_REUSEPORT và quota client là một giả thuyết cho chênh lệch fixed-request lớn hơn chênh lệch CPU/request; chưa cô lập đầy đủ nguyên nhân bằng trace assignment. Timed không phủ nhận fixed-request: hai ca fixed vẫn giảm 7–12%, nên chưa tuyên bố không có regression trên mọi workload.", "",
        "Control tốc độ ghi counters nhưng không scrape trong khoảng đo. Chi phí exposition chỉ được exercise trong test HTTP và các stage memory; chưa có benchmark throughput dưới nhiều scraper đồng thời.", ""]
    lines += ["## Các thử nghiệm đã giữ lại", "",
        "Image thử `6f9c17bf6fca` mặc định TLS 16 KiB (`test-results/metrics-header-control`, `metrics-heavy-control`) giảm RAM nhưng header đầy đủ giảm throughput 7,1%; hai ca body 64 KiB giảm 13,3–15,4%. Không dùng cấu hình đó làm mặc định cuối. Đã giữ 64 KiB, thêm tùy chọn 16 KiB và tối ưu counter cho worker có một writer. Không quy mọi khác biệt giữa các lần chạy thành tác dụng của code; CPU/p99/throughput trên host này còn dao động.", "",
        "Thử MALLOC_ARENA_MAX=2 có raw riêng `test-results/memory-arena2-control`, chỉ là một run khám phá; chưa dùng làm deployment default và không dùng để kết luận tiết kiệm RAM vì thiếu đối chứng CPU đầy đủ.", "",
        f"Raw memory: `{args.memory}`, heap trước/sau: `{args.heap_before}`, `{args.heap_after}`. Chưa soak 24 giờ hoặc định lượng allocator fragmentation bằng allocator-native counters. Các live pool gauges chỉ đếm pool groups được snapshot tham chiếu, không phải socket hay pool hit rate.", "",
        "Cách tái hiện: [memory-profiling.md](memory-profiling.md). Định nghĩa metrics và nhãn: [observability.md](observability.md).", ""]
    args.output.write_text("\n".join(lines),encoding="utf-8")
    print(args.output)


if __name__ == "__main__": main()
