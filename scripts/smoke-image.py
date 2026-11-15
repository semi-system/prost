"""Smoke a selected image in its own temporary Compose project and state volume."""
import argparse
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

ROOT=Path(__file__).resolve().parent.parent
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("--image",default="prost:bench-pgo")
args=parser.parse_args()
project="prost-image-smoke-"+uuid.uuid4().hex[:8]
with tempfile.TemporaryDirectory(prefix="prost-image-smoke-") as directory:
    override=Path(directory)/"override.yaml"
    override.write_text(f'''services:
  prost:
    image: {args.image}
    ports: !override
      - "127.0.0.1:18088:8080"
      - "127.0.0.1:19000:9000"
    environment:
      TOKIO_WORKER_THREADS: 2
''')
    compose=["docker","compose","-p",project,"-f",str(ROOT/"compose.yaml"),"-f",str(override)]
    try:
        subprocess.run([*compose,"up","-d","--no-build"],check=True)
        deadline=time.monotonic()+20
        while True:
            try:
                # CLI-only test fixture URL, controlled by the test/operator; no remote user input or production TLS artifact fetching.
                # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected,python.lang.security.audit.insecure-transport.urllib.insecure-urlopen.insecure-urlopen
                with urllib.request.urlopen("http://127.0.0.1:18088/",timeout=2) as response:
                    if response.status==200:
                        break
            except OSError:
                pass
            if time.monotonic()>=deadline:
                raise RuntimeError("smoke image did not become ready")
            time.sleep(0.1)
        subprocess.run([sys.executable,str(ROOT/"scripts/smoke.py"),
                        "--compose-file",str(ROOT/"compose.yaml"),"--compose-file",str(override),
                        "--project-name",project,"--http-url","http://127.0.0.1:18088/","--tcp-port","19000"],check=True)
    finally:
        subprocess.run([*compose,"down","-v"],check=True)
