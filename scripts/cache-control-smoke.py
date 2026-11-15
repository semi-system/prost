"""Local Docker/WSL release CLI cache inspection/purge/lifecycle checks.

An independent backend counter distinguishes real cache hits from origin fetches.
Uses only UUID containers/network and fresh output; this is not a benchmark.
"""
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
    parser.add_argument("--output-dir",type=Path,default=ROOT/"test-results/cache-control-smoke")
    args=parser.parse_args()
    out=args.output_dir.resolve();out.mkdir(parents=True,exist_ok=False)
    name="prost-cache-control-"+uuid.uuid4().hex[:10];backend=name+"-backend"
    seed='''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
io_workers=2
drain_seconds=1
[upstreams.web.http.cache]
max_bytes=1048576
max_entries=16
max_object_bytes=131072
max_ttl_seconds=60
[[upstreams.web.endpoints]]
id="one"
address="backend:8080"
[[upstreams.disabled.endpoints]]
id="one"
address="backend:8080"
[[listeners]]
id="http"
address="0.0.0.0:8080"
protocol="http"
upstream="web"
[[listeners.plugins]]
name="headers"
config={response={set={x-policy-generation="0"}}}
'''
    (out/"seed.toml").write_text(seed)
    created=[];clients=[];commands=[];checks=[]
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
    def published(container):
        info=json.loads(run("docker","inspect",container,capture_output=True,text=True).stdout)[0]
        return info,int(info["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"])
    def connection(port):
        client=HTTPConnection("127.0.0.1",port,timeout=5);clients.append(client);return client
    def origin_count():
        origin.request("GET","/stats")
        response=origin.getresponse();assert response.status==200
        return json.loads(response.read())["requests"]
    def get(path="/cached/a",encoding="identity",hit=None,generation="0",only=False):
        headers={"host":"cache.test","accept-encoding":encoding}
        if only:headers["cache-control"]="only-if-cached"
        frontend.request("GET",path,headers=headers)
        response=frontend.getresponse();body=response.read()
        if only:
            assert response.status==504
            return
        assert response.status==200 and body==b"0123456789abcdef"*4096
        assert response.getheader("x-policy-generation")==generation
        if hit is not None:assert (response.getheader("age") is not None)==hit
    def status():return ctl("cache","status","web")["caches"]["web"]["status"]
    run("docker","network","create",name,stdout=subprocess.DEVNULL)
    try:
        created.append(backend)
        run("docker","run","-d","--name",backend,"--network",name,"--network-alias","backend","--cpus","1","--memory","128m","-e","GOMAXPROCS=1","-e","BACKEND_LABEL=origin","-v",f"{args.cert_dir.resolve()}:/certs:ro","-p","127.0.0.1::8080",args.backend_image,stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker","run","-d","--name",name,"--network",name,"--cpus","2","--memory","256m","-e","TOKIO_WORKER_THREADS=2","-v",f"{out}:/cases:ro","-v",f"{out/'seed.toml'}:/etc/prost/seed.toml:ro","-p","127.0.0.1::8080",args.image,stdout=subprocess.DEVNULL)
        ready()
        info,port=published(name);_,origin_port=published(backend)
        frontend=connection(port);origin=connection(origin_port)
        get(hit=False);get(hit=True);assert origin_count()==1
        get(encoding="gzip",hit=False);get(encoding="gzip",hit=True);get("/cached/b",hit=False)
        assert origin_count()==3 and status()["entries"]==3
        assert ctl("cache","status")["caches"]["disabled"]["enabled"] is False
        key=ctl("cache","key","--uri","/cached/a","--host","cache.test","--accept-encoding","identity")["key"]
        entries=ctl("cache","entries","web","--key",key.upper())["cache"]["entries"]
        assert len(entries)==1 and entries[0]["key"]==key
        assert ctl("cache","entries","web","--limit","1")["cache"]["truncated"] is True
        checks.append("origin counters prove hits, encoding variants and exact administrative key")
        ctl("cache","purge","web",failure="required")
        ctl("cache","purge","web","--key",key,"--all",failure="cannot be used")
        ctl("cache","purge","web","--key","bad","--if-generation","0",failure="64 hex digits")
        ctl("cache","purge","web","--all","--if-generation","99",failure="generation_conflict")
        ctl("cache","purge","disabled","--all","--if-generation","0",failure="cache is disabled")
        assert status()["epoch"]==0
        purged=ctl("cache","purge","web","--key",key)
        assert purged["purge"]["removed_entries"]==1 and purged["purge"]["epoch"]==1 and purged["ephemeral"] is True
        get("/cached/b",hit=True);get(encoding="gzip",hit=True);assert origin_count()==3
        get(hit=False);get(hit=True);assert origin_count()==4
        purged=ctl("cache","purge","web","--all","--if-generation","0")
        assert purged["purge"]["removed_entries"]==3 and purged["purge"]["epoch"]==2
        get(only=True);assert origin_count()==4
        get(hit=False);assert origin_count()==5
        checks.append("scope/generation validation, exact purge preserves other variants and all purge invalidates lookup")
        (out/"candidate.toml").write_text(seed.replace('x-policy-generation="0"','x-policy-generation="1"'))
        ctl("apply","--file","/cases/candidate.toml","--if-generation","0")
        # A rolling generation closes old keepalive after its current response.
        frontend.close();frontend=connection(port)
        assert status()["epoch"]==2
        get(hit=True,generation="1");assert origin_count()==5
        ctl("cache","purge","web","--all","--if-generation","0",failure="generation_conflict")
        ctl("cache","purge","web","--all","--if-generation","1")
        get(hit=False,generation="1");assert origin_count()==6
        checks.append("compatible apply preserves cache; native response headers use new generation")
        frontend.close()
        run("docker","restart",name,stdout=subprocess.DEVNULL)
        assert ready()["generation"]==1
        info,port=published(name);frontend=connection(port)
        assert status()["epoch"]==0 and status()["entries"]==0
        get(hit=False,generation="1");get(hit=True,generation="1");assert origin_count()==7
        checks.append("restart keeps committed cache configuration and starts fresh ephemeral state")
        summary={"state":"passed","image_id":info["Image"],"checks":checks,"origin_requests":origin_count(),"final_cache":status(),"method":"functional CLI correctness checks; no throughput measurement"}
        (out/"summary.json").write_text(json.dumps(summary,indent=2)+"\n");print(json.dumps(summary,indent=2),flush=True)
    finally:
        (out/"commands.json").write_text(json.dumps(commands,indent=2)+"\n")
        for client in clients:client.close()
        for container in reversed(created):
            with (out/f"{container}.log").open("w") as log:subprocess.run(["docker","logs",container],stdout=log,stderr=subprocess.STDOUT,timeout=30)
            subprocess.run(["docker","rm","-f","-v",container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        subprocess.run(["docker","network","rm",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)

if __name__=="__main__":main()
