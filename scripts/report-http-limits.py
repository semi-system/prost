"""Render recorded HTTP limits regression, enabled-policy and memory results."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def read(name):
    return json.loads((ROOT / "test-results" / name / "summary.json").read_text())

def section(title, name):
    data = read(name)
    window = f"{data['duration_seconds']} giây/lượt" if data["duration_seconds"] else "quota request cố định"
    lines = [f"## {title}", "", f"Raw: `test-results/{name}/`; {data['rounds']} rounds, {window}.", "",
        "| Workload | RPS trước → sau | Δ RPS | p99 ms trước → sau | CPU µs/request trước → sau | Peak cgroup WS MiB trước → sau | Gate |",
        "|---|---:|---:|---:|---:|---:|---|"]
    failed = []
    for case, result in data["results"].items():
        a, b, gate = result["before"], result["after"], result["gate"]
        flag = "CHECK" if gate["investigation_required"] else "pass"
        if gate["investigation_required"]: failed.append(case)
        lines.append(f"| {case} | {a['rps']:.0f} → {b['rps']:.0f} | {gate['throughput_delta_percent']:+.2f}% | {a['p99_ms']:.3f} → {b['p99_ms']:.3f} | {a['cpu_us']:.2f} → {b['cpu_us']:.2f} | {a['working_set_mib']:.2f} → {b['working_set_mib']:.2f} | {flag} |")
    before = sum(r["before"]["requests"] for r in data["results"].values())
    after = sum(r["after"]["requests"] for r in data["results"].values())
    errors = sum(r[label]["failures"] for r in data["results"].values() for label in ["before", "after"])
    tail = sum(r[label]["unfinished_at_deadline"] for r in data["results"].values() for label in ["before", "after"])
    lines += ["", f"Hoàn tất {before:,} request trước / {after:,} request sau; lỗi đã ghi nhận: {errors}. Stream chưa hoàn tất tại deadline: {tail:,} (tách riêng, không tính thành request thành công)."]
    if failed:
        lines += ["", "Vượt gate ở " + ", ".join(f"`{case}`" for case in failed) + ". Giữ kết quả này để điều tra; không coi là đã đạt toàn bộ gate."]
    first = next(iter(data["results"].values()))
    lines += ["", f"Image trước: `{first['before']['image_id']}`; sau: `{first['after']['image_id']}`.", "",
        "Config SHA256 trước/sau: " + " / ".join(f"`{value}`" for value in data["config_sha256_by_label"].values()) + ".", ""]
    return lines

def main():
    lines = ["# Đo streaming limits và granular timeouts", "",
        "Chạy Docker trong WSL, release Rust 1.93/thin LTO; WASM tắt. Khi đo throughput, proxy và backend mỗi bên 2 CPU/256 MiB. Khi profile RSS, proxy dùng 2 CPU/512 MiB và backend 2 CPU/256 MiB. Client/proxy/backend pin vào các guest core riêng ở lượt throughput; chỉ một proxy chạy mỗi lần, đảo thứ tự ảnh và workload theo round. Cùng backend/config/pool, không bật cache. Đây là phép đo trong máy phát triển, không phải cam kết production hay so sánh nginx mới.", "",
        "Gate: throughput giảm trên 5% hoặc p99 tăng trên 10% cần điều tra. CPU lấy từ cgroup chia request hoàn tất; WS là working set cgroup, không phải RSS riêng tiến trình. Không build hay chạy profiler đồng thời với throughput test.", "",
        "Fixed request có thể chịu ảnh hưởng đuôi connection và phân phối reuseport. Timed run giữ workload trong cửa sổ ghi ở từng phần, ghi riêng stream còn dở ở deadline. Các phương pháp đều được giữ lại; kết quả mới không xóa kết quả cũ.", ""]
    compact = all((ROOT / "test-results" / name / "summary.json").exists() for name in ["http-limits-compact-default", "http-limits-compact-policy", "http-limits-compact-memory"])
    if compact:
        lines += ["Bản hiện tại giữ TCP upstream trong enum nhỏ, đưa TLS/timer ghi tùy chọn ra allocation riêng theo connection, và tách future của nhánh policy khỏi future chuyển tiếp mặc định. Header timer nằm inline trong future policy; khi headers deadline tắt và không theo dõi upload thì gửi thẳng vào client pool. Các phép đo dưới đây so bản timeout đầu tiên với bản tối ưu, không suy ra lợi ích từ việc cộng phần trăm của các phiên khác nhau.", ""]
        lines += section("Bản tối ưu hiện tại — policy mới chưa bật", "http-limits-compact-default")
        lines += section("Bản tối ưu hiện tại — cùng policy được bật ở cả hai bản", "http-limits-compact-policy")
        if (ROOT / "test-results/http-limits-compact-tls-focused/summary.json").exists():
            lines += section("Kiểm tra riêng TLS H1 sau gate p99 ở lượt hỗn hợp", "http-limits-compact-tls-focused")
            lines += ["Lượt TLS riêng giữ đúng hai image/config như lượt hỗn hợp, tăng lên 5 round × 10 giây và chỉ chạy workload TLS H1. Kết quả riêng không xóa hàng CHECK trong workload hỗn hợp; hai cách đo giúp đánh giá độ ổn định của regression.", ""]
        current_memory = read("http-limits-compact-memory")
        groups = current_memory["results"]
        lines += ["## RSS của bản tối ưu", "", "Ba round, mỗi profile 10.000 HTTP + 1.000 TLS request; 128 idle clients, 32 slow TLS streams, 10 apply rồi cancel/pool expiry. Không dùng heaptrack.", "",
            "| Stage | Bản timeout đầu MiB | Tối ưu mặc định MiB | Tối ưu bật policy MiB |", "|---|---:|---:|---:|"]
        for stage in groups["before"]["stages"]:
            values = [groups[label]["stages"][stage]["rss_mib"] for label in ["before", "after", "after_limits"]]
            lines.append(f"| {stage} | " + " | ".join(f"{value:.2f}" for value in values) + " |")
        for label, group in groups.items():
            last = group["stages"]["after_pool_expiry"]
            lines += ["", f"`{label}`: FD cuối {last['fds']:.0f}, generations cuối {last['live_generations']}; image `{group['image_id']}`."]
        lines += ["", "RSS sau cancel của bản mặc định có stage cao hơn bản trước; không cam kết RAM giảm ở mọi thời điểm. Allocator có thể giữ trang sau hủy. Harness kiểm tra 0 connection và 1 generation ở cuối từng profile; dữ liệu này chưa thay thế soak test dài hạn."]
        lines += ["", "Các phép đo dưới đây được giữ để kiểm tra biến động và lịch sử điều tra, gắn với image ID của từng lượt.", ""]
    if (ROOT / "test-results/http-limits-focused/summary.json").exists():
        lines += section("Đối chứng lại trước tối ưu — chỉ header nhỏ và H2 64 KiB", "http-limits-focused")
        lines += ["Lượt đối chứng này vẫn dùng đúng hai binary ban đầu, trước khi sửa layout. Mức giảm ở phép đo hỗn hợp ban đầu không tái hiện ổn định ở lượt focused này. Có container ngoài dự án đang chạy (mẫu docker stats khoảng 9–36% CPU); giữ nguyên container đó, ghi nhận ở `test-results/http-limits-focused/context.json`. Không lấy việc kết quả đảo dấu làm bằng chứng đã giải quyết nguyên nhân regression.", ""]
    lines += section("Code mới, config mặc định — fixed request", "http-limits-default-fixed")
    lines += section("Code mới, config mặc định — timed", "http-limits-default-timed")
    lines += section("Cùng binary, bật toàn bộ policy — timed", "http-limits-policy-timed")
    lines += ["Policy bật: upload tối đa 8 MiB; idle upload/response-body, response headers và socket write hai phía 5 giây; header-read 5 giây. Request nhỏ rỗng không tạo upload state. Upload 1 MiB đi qua bộ đếm/timer streaming; không buffer cả body.", ""]
    memory = read("http-limits-memory")
    groups = memory["results"]
    lines += ["## RSS và thu hồi tài nguyên", "",
        f"{memory['rounds']} round, mỗi profile {memory['requests_per_profile']:,} HTTP + 1.000 TLS request, 128 idle clients, 32 TLS streams 16 MiB bị đọc chậm, 10 lần apply, hủy client rồi chờ pool expire. Process mới mỗi profile; không heaptrack/allocator override.", "",
        "| Stage | RSS trước MiB | RSS mới mặc định MiB | RSS bật policy MiB |",
        "|---|---:|---:|---:|"]
    for stage in groups["before"]["stages"]:
        values = [groups[label]["stages"][stage]["rss_mib"] for label in ["before", "after", "after_limits"]]
        lines.append(f"| {stage} | " + " | ".join(f"{value:.2f}" for value in values) + " |")
    lines += ["", "RSS là median giữa các process; allocator có thể giữ trang sau hủy nên không suy diễn RSS phải giảm ngay. Harness assert connection cuối bằng 0 và live generation bằng 1.", ""]
    for label, group in groups.items():
        last = group["stages"]["after_pool_expiry"]
        lines.append(f"- `{label}`: FD cuối {last['fds']:.0f}; generations cuối {last['live_generations']}; image `{group['image_id']}`.")
    lines += ["", "Raw RSS/FD/metrics/status theo từng stage ở `test-results/http-limits-memory/`. Kiểm thử và ngữ nghĩa cấu hình: [http-limits.md](http-limits.md). Không đo chính xác từng H2 stream bị kẹt WINDOW_UPDATE bằng socket write timeout; không dùng kết quả này để tuyên bố đã có deadline đó.", ""]
    cpu = ROOT / "test-results/http-limits-cpu-linux"
    if (cpu / "1.symbols.json").exists():
        profiles = [json.loads((cpu / f"{index}.symbols.json").read_text()) for index in range(2)]
        lines += ["## CPU profile để điều tra regression", "",
            "Lấy mẫu `cpu-clock:u` 199 Hz, workload H2 64 KiB, ngoài lượt benchmark. Perf ghi trực tiếp lên `/mnt/d` bị lỗi Bad address; chạy lại trên filesystem Linux rồi copy artifact thành công. Bản perf của WSL thiếu libunwind/libdw và không giải mã ELF; dùng symbol table của đúng image ID để đối chiếu leaf IP. Không có call stack/inline đầy đủ; tỷ lệ dưới đây là self samples gần đúng, không phải CPU/request.", "",
            "| Vùng CPU | Trước | Sau |", "|---|---:|---:|"]
        for name in ["_aesni_ctr32_ghash_6x", "prost_core::RunningProxy::handle_http::{{closure}}::{{closure}}"]:
            values = [dict(profile["functions"]).get(name, 0) for profile in profiles]
            lines.append(f"| `{name}` | {values[0]:.2f}% | {values[1]:.2f}% |")
        values = [profile["dso_percent"].get("libc.so.6", 0) for profile in profiles]
        lines += [f"| libc tổng | {values[0]:.2f}% | {values[1]:.2f}% |", "",
            "Hai IO worker đều gần bão hòa trong các lượt timed. Profile ban đầu chưa định vị được nguyên nhân chính xác: phân bố self samples không lộ một hàm mới chiếm tỷ trọng lớn. Không suy diễn rằng vì vậy không có regression. Các hàng CHECK giữ nguyên ý nghĩa cho đúng image và lượt đo tương ứng; thay layout và đo lại được báo riêng ở đầu tài liệu khi có kết quả.", "",
            "Raw: `test-results/http-limits-cpu-linux/`; công cụ: `scripts/profile-http-cpu.py` và `scripts/symbolize-http-cpu.py`. Throughput của lượt có profiler không được dùng trong các bảng benchmark.", ""]
    destination = ROOT / "docs/http-limits-benchmark.md"
    destination.write_text("\n".join(lines))
    print(destination)

if __name__ == "__main__":
    main()
