"""Generate the load-balancing report from saved measurements; no traffic is sent."""
import argparse
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
LABELS={"rr_uniform":"Round-robin, backend đồng đều","least_uniform":"Least-conn, backend đồng đều",
        "rr_slow":"Round-robin, một backend chậm 5 ms","least_slow":"Least-conn, một backend chậm 5 ms",
        "hash_uniform":"Consistent hash, backend đồng đều",
        "rr_backup":"Round-robin, primary cap 16 + ba backup",
        "least_backup":"Least-conn, primary cap 16 + ba backup",
        "rr_passive":"Round-robin, passive policy bật",
        "least_passive":"Least-conn, passive policy bật"}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory",type=Path,required=True)
    parser.add_argument("--control",type=Path)
    parser.add_argument("--previous-control",type=Path)
    parser.add_argument("--date",default="2026-11-12")
    parser.add_argument("--output",type=Path,default=ROOT/"docs/load-balancing-benchmark.md")
    args=parser.parse_args()
    data=json.loads((args.directory/"summary.json").read_text())
    rows=data["results"];meta=data["metadata"]
    requests=sum(r["prost"]["requests"] for r in rows.values())
    failures={t:sum(r[t]["failures"] for r in rows.values()) for t in ("prost","nginx")}
    lines=[f"# Cân bằng tải với bốn backend — {args.date}","",
        f"Mỗi proxy hoàn tất **{requests:,} request đo**, {meta['rounds']} vòng; lỗi Prost {failures['prost']}, nginx {failures['nginx']}.","",
        f"Raw: `{args.directory}`. Throughput/p99/CPU dùng trung vị; RAM là đỉnh working set lấy mẫu lớn nhất.","",
        "## Điều kiện", "",
        "- Docker trong WSL2, cùng fixture Go cho bốn backend riêng biệt. Mỗi proxy 2 CPU/256 MiB, mỗi backend 1 CPU/128 MiB, GOMAXPROCS=1.",
        "- HTTPS/2 downstream, HTTP/1.1 upstream, body cố định 34 byte; 32 connection × 4 stream, 2 load threads; warmup 256 request.",
        "- Proxy và case chạy luân phiên; không build hay chạy proxy đối thủ đồng thời. Client/server vẫn cùng host, không có load generator riêng.",
        "- Không cache, access log, response/request buffering hoặc retry. nginx dùng zone để chia sẻ trạng thái giữa worker; Prost có active /ready checks, không tính vào counter traffic của fixture.",
        "- Idle budget 512 qua bốn địa chỉ: Prost tối đa 128/host, nginx 256/worker cho cả group. Phân bổ per-host và shared-group khác nhau, là một phần của workload.",
        "- Trước tải kiểm tra byte body, protocol upstream và affinity mẫu; từng request đo phải có status 200, đủ mẫu latency và đủ byte. Counter bốn backend phải cộng đúng số request đo.",
        "- Counter /stats không tính health/readiness và chính nó. Metadata cgroup/image ID/config đã lưu để kiểm tra budget và backend thực sự nhận traffic.","",
        "## Throughput và latency", "",
        "| Workload | Prost req/s | nginx req/s | Chênh Prost | p99 Prost / nginx ms |","|---|---:|---:|---:|---:|"]
    affinity=meta.get("cpu_affinity")
    if affinity:
        lines.insert(lines.index("## Throughput và latency"),"CPU affinity trong guest: client `"+affinity["client"]+"`, proxy `"+affinity["proxy"]+"`, bốn backend `"+str(affinity["backends"])+"`. Chọn logical CPU đầu tiên của các core khác nhau theo lscpu; không bảo đảm cô lập vật lý khỏi Windows/hypervisor.\n")
    for case,row in rows.items():
        p,n=row["prost"],row["nginx"]
        lines.append(f"| {LABELS.get(case,case)} | {p['rps']:,.0f} | {n['rps']:,.0f} | {(p['rps']/n['rps']-1)*100:+.1f}% | {p['p99_ms']:.2f} / {n['p99_ms']:.2f} |")
    if any(case.endswith(("_backup", "_passive")) for case in rows):
        lines += ["", "Biên throughput giữa các vòng (req/s); các lần đo ngắn trên cùng host còn dao động:", "",
            "| Workload | Prost min–max | nginx min–max |", "|---|---:|---:|"]
        for case,row in rows.items():
            p,n=row["prost"]["rps_rounds"],row["nginx"]["rps_rounds"]
            lines.append(f"| {LABELS.get(case,case)} | {min(p):,.0f}–{max(p):,.0f} | {min(n):,.0f}–{max(n):,.0f} |")
    lines += ["","## CPU và RAM", "","| Workload | CPU µs/request Prost / nginx | Working set MiB Prost / nginx |","|---|---:|---:|"]
    for case,row in rows.items():
        p,n=row["prost"],row["nginx"]
        lines.append(f"| {LABELS.get(case,case)} | {p['cpu_us']:.2f} / {n['cpu_us']:.2f} | {p['working_set_mib']:.2f} / {n['working_set_mib']:.2f} |")
    lines += ["","Working set là memory.current trừ inactive_file, không phải RSS/heap riêng. Mẫu cách 50 ms có thể bỏ lỡ spike. Các case dùng process proxy mới nên không so RAM trực tiếp với các bộ tải khác có vòng đời/buffer khác.","",
        "## Phân phối tới backend", "","| Workload / proxy | node-a | node-b | node-c | node-d |","|---|---:|---:|---:|---:|"]
    for case,row in rows.items():
        for t in ("prost","nginx"):
            c=row[t]["backend_requests"]
            lines.append(f"| {case} / {t} | {c['node-a']:,} | {c['node-b']:,} | {c['node-c']:,} | {c['node-d']:,} |")
    if "least_slow" in rows and "rr_slow" in rows:
        p=rows["least_slow"]["prost"];r=rows["rr_slow"]["prost"]
        lines += ["",f"Prost least-conn giảm phần request vào node-a chậm từ {100*r['backend_requests']['node-a']/r['requests']:.1f}% xuống {100*p['backend_requests']['node-a']/p['requests']:.1f}%. Throughput thay đổi {(p['rps']/r['rps']-1)*100:+.1f}% so với round-robin, nên không suy ra rằng chuyển tải khỏi node chậm luôn tăng throughput/p99. Tải này còn gồm TLS, scheduling, pool và chi phí giữ load lease."]
    if "hash_uniform" in rows:
        lines += ["",f"Hash dùng corpus {meta['hash_key_corpus']} request-URI; driver tuần hoàn URI trên từng client. Preflight kiểm tra lặp lại 16 key. SHA-256/ring của Prost và ketama của nginx cho mapping khác nhau, như bảng phân phối cho thấy. Các backend hash đều không có delay nhân tạo, nhưng đây vẫn là phép đo toàn bộ routing/pool, không cô lập chi phí hàm hash.",""]
    if any(case.endswith("_backup") for case in rows):
        lines += ["", "## Primary giới hạn và backend dự phòng", "",
            "node-a là primary duy nhất, delay trước response header 5 ms, cap 16. node-b/c/d là backup không đặt cap riêng. Prost dùng max_active=16, nginx max_conns=16 + zone. Upstream HTTP/1.1 không multiplex; đây vẫn là hai đơn vị cap khác nhau, không suy rộng sang HTTP/2 upstream.", "",
            "Preflight tuần tự phải chỉ vào primary. Tải 128 stream phải có request vào cả primary và backup; counters backend phải cộng đủ số request và mọi status/body phải đúng. Đây là tải spillover khi primary đầy, không phải lỗi backend/crash hay benchmark thời gian phát hiện health failure.", ""]
    if any(case.endswith("_passive") for case in rows):
        lines += ["", "## Passive policy trên tải healthy", "",
            "Bốn backend trả 200. Prost bật max_fails=3, fail_timeout_ms=1000, failure_statuses=[502,503,504]. nginx dùng zone, max_fails=3 fail_timeout=1s và proxy_next_upstream error timeout http_502 http_503 http_504 với proxy_next_upstream_tries=1 để không replay request. Tổng counter backend phải đúng số request client.", "",
            "Đây là đo đường chạy healthy khi policy đã bật, không đo độ trễ ejection/recovery. Hai bên không có health semantics giống hệt: Prost đếm trong cửa sổ trượt và yêu cầu active probe sau cooldown; nginx OSS cho thử traffic lại theo fail_timeout. Các test fault HTTP/TCP và CLI được chạy riêng; không suy ra failover nhanh hơn từ bảng req/s này.", ""]
    if args.control:
        control=json.loads(args.control.read_text())
        rounds=control.get("rounds",3)
        count=next(iter(control["results"].values()))["before"]["requests"]//rounds
        lines += ["## Đối chứng đường chạy mặc định", "",
                  f"So image trước/sau trên cùng config không dùng thuật toán mới, luân phiên thứ tự image/case. Mỗi ca/vòng {count:,} request, {rounds} vòng; raw: `"+str(args.control.parent)+"`.","",
                  "| Control | Throughput trước → sau | Chênh | p99 trước → sau ms |","|---|---:|---:|---:|"]
        for case,row in control["results"].items():
            a,b=row["before"],row["after"]
            lines.append(f"| {case} | {a['rps']:,.0f} → {b['rps']:,.0f} | {(b['rps']/a['rps']-1)*100:+.1f}% | {a['p99_ms']:.2f} → {b['p99_ms']:.2f} |")
        exceeded=[case for case,row in control["results"].items() if row["gate"]["investigation_required"]]
        lines += ["", "Gate throughput giảm trên 5% hoặc p99 tăng trên 10%: "+("cần điều tra "+", ".join(exceeded) if exceeded else "không có ca vượt ngưỡng trong phép đo này")+".",""]
        if control.get("cpu_affinity"):
            lines += ["CPU affinity của đối chứng: `"+str(control["cpu_affinity"])+"`. Không so p99 tuyệt đối giữa phép đối chứng và các lần đo không ghim CPU.",""]
        if args.previous_control:
            earlier=json.loads(args.previous_control.read_text())
            for case,row in earlier["results"].items():
                if row["gate"]["investigation_required"]:
                    pinning = "có ghim CPU" if earlier.get("cpu_affinity") else "không ghim CPU"
                    lines += [f"Đối chứng trước {pinning} (`{args.previous_control.parent}`) ghi nhận {case}: throughput {row['gate']['throughput_delta_percent']:+.1f}%, p99 {row['gate']['p99_delta_percent']:+.1f}%. Đã điều tra nhánh round-robin và điều chỉnh fast path trước lần đo hiện tại. Giữ raw của lần vượt ngưỡng để kiểm tra; không quy mọi chênh lệch giữa các lần đo thành hiệu quả của code, vì tải cùng host vẫn dao động.",""]
    if "environment" in meta:
        env=meta["environment"]
        lines += ["## Môi trường", "", f"CPU: {env['cpu']}; kernel `{env['kernel']}`; `{env['h2load']}`.",
            f"Phiên bản: `{env.get('versions',{}).get('prost','unknown')}`; `{env.get('versions',{}).get('nginx','unknown')}`.",""]
    lines += [f"Source SHA-256: `{meta['source_sha256']}`. Các JSON từng vòng ghi image ID, limits, phân phối và mẫu cgroup.","",
        "Kết quả chỉ bao phủ fixture nhỏ này; chưa thay thế workload body lớn, soak 24 giờ hoặc profiling production. Hành vi và giới hạn: [load-balancing.md](load-balancing.md). Lệnh tái hiện: [benchmarks/README.md](../benchmarks/README.md).",""]
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text("\n".join(lines),encoding="utf-8")
    print(args.output)


if __name__=="__main__":main()
