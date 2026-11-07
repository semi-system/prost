"""Opt-in PGO build, trained on small HTTP/1.0 and HTTP/1.1 proxy traffic.

Run in WSL as root after building prost:mvp and prost-dev. Raw profiles and
compilation caches stay in separate Linux Docker volumes. This is workload-
specific optimization, not a claim about TLS/WASM/large-body performance.
"""
import argparse
import hashlib
import json
from pathlib import Path
import runpy
import subprocess
import tempfile

ROOT=Path(__file__).resolve().parent.parent
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("--stage",choices=["all","generate","train","use"],default="all")
args=parser.parse_args()


def run(*command,**kwargs):
    return subprocess.run(command,check=True,**kwargs)


sources=[ROOT/"Cargo.toml",ROOT/"Cargo.lock"]+sorted((ROOT/"crates").rglob("*.rs"))+sorted((ROOT/"crates").rglob("Cargo.toml"))
digest=hashlib.sha256()
for path in sources:
    digest.update(str(path.relative_to(ROOT)).encode())
    digest.update(path.read_bytes())
version=digest.hexdigest()[:16]
raw=f"/profiles/{version}/raw"
merged=f"/profiles/{version}/merged.profdata"
output=ROOT/"test-results/pgo"/version
output.mkdir(parents=True,exist_ok=True)
bench=runpy.run_path(str(ROOT/"scripts/compare-proxies.py"))
target="x86_64-unknown-linux-gnu"


def export_image(container,tag):
    with tempfile.TemporaryDirectory(prefix="prost-pgo-image-") as directory:
        context=Path(directory)
        for binary in ("prost","prostctl"):
            run("docker","cp",f"{container}:/pgo-target/{target}/release/{binary}",str(context/binary))
        (context/"Dockerfile").write_text("FROM prost:mvp\nCOPY prost prostctl /usr/local/bin/\n")
        run("docker","build","-t",tag,str(context))


def compile_profile(flag,tag):
    name="prost-pgo-compile"
    try:
        run("docker","run","--name",name,
            "-v",f"{ROOT}:/work","-v","prost-pgo-target:/pgo-target",
            "-v","prost-pgo-data:/profiles","-v","prost-cargo:/usr/local/cargo/registry",
            "-w","/work","-e","CARGO_TARGET_DIR=/pgo-target","-e","CARGO_BUILD_JOBS=2",
            "-e",f"RUSTFLAGS={flag}","prost-pgo-dev",
            "cargo","build","--release","--workspace","--locked","--target",target)
        export_image(name,tag)
    finally:
        subprocess.run(["docker","rm",name],check=False)


if args.stage in ("all","generate"):
    run("docker","build","-f",str(ROOT/"Dockerfile.pgo"),"-t","prost-pgo-dev",str(ROOT))
    run("docker","run","--rm","-v","prost-pgo-data:/profiles","prost-pgo-dev",
        "sh","-c",f"mkdir -p {raw} && chmod 777 {raw}")
    compile_profile(f"-Cprofile-generate={raw}","prost:pgo-generate")

if args.stage in ("all","train"):
    with tempfile.TemporaryDirectory(prefix="prost-pgo-train-") as directory:
        override=Path(directory)/"override.json"
        override.write_text(json.dumps({"services":{"prost-local":{
            "image":"prost:pgo-generate",
            "environment":{"LLVM_PROFILE_FILE":f"{raw}/%m-%p.profraw"},
            "volumes":["prost-pgo-data:/profiles"]}},
            "volumes":{"prost-pgo-data":{"external":True,"name":"prost-pgo-data"}}}))
        bench["COMPOSE"].extend(["-f",str(override)])
        compose=bench["compose"]
        try:
            compose("up","-d","backend")
            compose("stop",*bench["TARGETS"])
            compose("up","-d","--no-deps","--force-recreate","--renew-anon-volumes","prost-local")
            bench["ready"](18085)
            result=run("ab","-q","-k","-n","50000","-c","100","http://127.0.0.1:18085/",capture_output=True,text=True)
            (output/"training-ab.txt").write_text(result.stdout)
            bench["load"](18085,"prost-bench-local","PGO training instrumented",output/"training-http11.json",driver="asyncio",requests=50000)
        finally:
            # A clean exit is needed to flush LLVM counters.
            compose("stop")
    sysroot=run("docker","run","--rm","prost-pgo-dev","rustc","--print","sysroot",capture_output=True,text=True).stdout.strip()
    run("docker","run","--rm","-v","prost-pgo-data:/profiles","prost-pgo-dev",
        f"{sysroot}/lib/rustlib/{target}/bin/llvm-profdata","merge","-o",merged,raw)

if args.stage in ("all","use"):
    run("docker","run","--rm","-v","prost-pgo-data:/profiles","prost-pgo-dev","test","-s",merged)
    compile_profile(f"-Cprofile-use={merged}","prost:bench-pgo")

(output/"manifest.json").write_text(json.dumps({"source_hash":version,"raw":raw,"merged":merged,
    "target":target,"rust":"1.93.1","target_cpu":"portable default; no native CPU flags",
    "training":"50000 HTTP/1.0 keepalive + 50000 HTTP/1.1 keepalive, JSON 34 bytes",
    "result_image":"prost:bench-pgo"},indent=2)+"\n")
print(f"PGO stage {args.stage} completed; source {version}; artifacts {output}")
