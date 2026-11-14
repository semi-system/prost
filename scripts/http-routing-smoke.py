"""Local Docker/WSL release-image method/header routing and CLI lifecycle checks."""
import argparse
from http.client import HTTPConnection
import json
from pathlib import Path
import subprocess
import time
import uuid

ROOT=Path(__file__).resolve().parent.parent

def run(*args,**kwargs):
    kwargs.setdefault("timeout",60)
    return subprocess.run(args,check=True,**kwargs)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image",default="prost:native")
    parser.add_argument("--backend-image",default="prost:bundle-backend")
    parser.add_argument("--cert-dir",type=Path,default=ROOT/"test-results/advanced-fixtures/certs")
    parser.add_argument("--output-dir",type=Path,default=ROOT/"test-results/http-routing-smoke")
    args=parser.parse_args()
    out=args.output_dir.resolve();out.mkdir(parents=True,exist_ok=False)
    name="prost-http-route-"+uuid.uuid4().hex[:10]
    seed='''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
io_workers=2
drain_seconds=1
[upstreams.blue]
[[upstreams.blue.endpoints]]
id="one"
address="blue:8080"
[upstreams.green]
[[upstreams.green.endpoints]]
id="one"
address="green:8080"
[[listeners]]
id="web"
address="0.0.0.0:8080"
protocol="http"
upstream="blue"
[[listeners.plugins]]
name="headers"
config={request={set={x-canary="green"}},response={set={x-policy-generation="0"}}}
[[listeners.routes]]
priority=20
methods=["GET","HEAD"]
headers=[{name="x-canary",equals="green",ignore_case=true}]
upstream="green"
[[listeners.routes]]
priority=15
methods=["GET"]
headers=[{name="x-tag",matches="^tenant-[0-9]+$"},{name="x-blocked",present=false}]
upstream="green"
[[listeners.routes]]
priority=10
methods=["POST"]
headers=[{name="x-region",prefix="EU-",ignore_case=true}]
upstream="green"
'''
    (out/"seed.toml").write_text(seed)
    created=[];clients=[];commands=[];checks=[];h2_probes=[];port=None
    def ctl(*command,failure=None):
        response=subprocess.run(["docker","exec",name,"prostctl",*command],capture_output=True,text=True,timeout=30)
        commands.append({"command":list(command),"code":response.returncode,"stdout":response.stdout,"stderr":response.stderr})
        if failure is not None:
            assert response.returncode!=0 and failure in response.stdout+response.stderr,commands[-1]
            return None
        response.check_returncode()
        return json.loads(response.stdout)["result"]
    def ready():
        for _ in range(100):
            try:return ctl("status")
            except subprocess.CalledProcessError:time.sleep(.1)
        raise RuntimeError("proxy did not start")
    def connection():
        client=HTTPConnection("127.0.0.1",port,timeout=5);clients.append(client);return client
    def get(client,method,headers,backend,generation="0",status=200):
        client.request(method,"/bytes/34",headers=headers)
        response=client.getresponse();body=response.read()
        assert response.status==status,(response.status,body)
        if status==200:
            assert response.getheader("x-backend")==backend
            assert response.getheader("x-policy-generation")==generation
            assert body==(b"" if method=="HEAD" else (b"0123456789abcdef"*3)[:34])
    def h2(value,backend):
        headers=out/f"h2-{len(h2_probes)+1}-{value}.headers"
        result=run("curl","--silent","--show-error","--fail","--max-time","10","--http2-prior-knowledge","-H",f"x-canary: {value}","-D",str(headers),f"http://127.0.0.1:{port}/bytes/34",capture_output=True)
        assert result.stdout==(b"0123456789abcdef"*3)[:34]
        raw=headers.read_text().lower()
        assert raw.startswith("http/2 200") and f"x-backend: {backend}" in raw,raw
        h2_probes.append({"key":value,"backend":backend,"headers":headers.name})
    run("docker","network","create",name,stdout=subprocess.DEVNULL)
    try:
        for label in ("blue","green"):
            backend=name+"-"+label;created.append(backend)
            run("docker","run","-d","--name",backend,"--network",name,"--network-alias",label,"--cpus","1","--memory","128m","-e","GOMAXPROCS=1","-e",f"BACKEND_LABEL={label}","-v",f"{args.cert_dir.resolve()}:/certs:ro",args.backend_image,stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker","run","-d","--name",name,"--network",name,"--cpus","2","--memory","256m","-e","TOKIO_WORKER_THREADS=2","-v",f"{out}:/cases:ro","-v",f"{out/'seed.toml'}:/etc/prost/seed.toml:ro","-p","127.0.0.1::8080",args.image,stdout=subprocess.DEVNULL)
        ready()
        info=json.loads(run("docker","inspect",name,capture_output=True,text=True).stdout)[0]
        port=int(info["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"])
        client=connection()
        for method,headers,backend in [("GET",{},"blue"),("GET",{"x-canary":"GREEN"},"green"),("HEAD",{"x-canary":"green"},"green"),
            ("POST",{"x-region":"eu-west"},"green"),("GET",{"x-region":"eu-west"},"blue"),
            ("GET",{"x-tag":"tenant-42"},"green"),("GET",{"x-tag":"tenant-42","x-blocked":""},"blue")]:
            get(client,method,headers,backend)
        h2("green","green")
        checks.append("H1/H2 methods, exact/prefix/regex/presence and incoming headers before native rewrite")
        candidate=seed.replace('equals="green"','equals="blue"').replace('x-policy-generation="0"','x-policy-generation="1"')
        (out/"candidate.toml").write_text(candidate)
        ctl("apply","--file","/cases/candidate.toml","--if-generation","0")
        get(client,"GET",{"x-canary":"green"},"blue","1")
        get(client,"GET",{"x-canary":"blue"},"green","1")
        h2("green","blue");h2("blue","green")
        (out/"invalid.toml").write_text(candidate.replace('matches="^tenant-[0-9]+$"','matches="("'))
        for command in ("validate","apply"):
            arguments=[command,"--file","/cases/invalid.toml"]
            if command=="apply":arguments.extend(["--if-generation","1"])
            ctl(*arguments,failure="route header regex")
        assert ctl("status")["generation"]==1
        checks.append("hot apply on existing keepalive; invalid regex validate/apply rollback")
        ctl("upstream","disable","green/one","--if-generation","1")
        get(client,"GET",{"x-canary":"blue"},None,"1",503)
        get(client,"GET",{},"blue","1")
        ctl("upstream","enable","green/one","--if-generation","2")
        checks.append("matched unavailable upstream returns 503; listener fallback remains independent")
        for client in clients:client.close()
        run("docker","restart",name,stdout=subprocess.DEVNULL)
        assert ready()["generation"]==3
        info=json.loads(run("docker","inspect",name,capture_output=True,text=True).stdout)[0]
        port=int(info["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"])
        get(connection(),"GET",{"x-canary":"blue"},"green","1")
        h2("blue","green")
        checks.append("restart restores committed method/header matchers")
        summary={"state":"passed","image_id":info["Image"],"checks":checks,"h2_probes":h2_probes,"method":"functional smoke only; no benchmark","final_generation":3}
        (out/"summary.json").write_text(json.dumps(summary,indent=2)+"\n");print(json.dumps(summary,indent=2),flush=True)
    finally:
        (out/"commands.json").write_text(json.dumps(commands,indent=2)+"\n")
        for client in clients:client.close()
        for container in reversed(created):
            with (out/f"{container}.log").open("w") as log:subprocess.run(["docker","logs",container],stdout=log,stderr=subprocess.STDOUT,timeout=30)
            subprocess.run(["docker","rm","-f","-v",container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        subprocess.run(["docker","network","rm",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)

if __name__=="__main__":main()
