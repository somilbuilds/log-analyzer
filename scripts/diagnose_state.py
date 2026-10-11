import subprocess
import json
import sys

def run_cmd(cmd, shell=True):
    p = subprocess.run(cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    return p.returncode, p.stdout, p.stderr

print("=== 1. PYTHON VERSIONS ===")
rc, out, err = run_cmd("docker exec namenode python3 --version")
print("namenode python3:", out.strip(), err.strip())
rc, out, err = run_cmd("docker exec nodemanager python3 --version")
print("nodemanager python3:", out.strip(), err.strip())

print("\n=== 2. TEST SYNTAX IN NODEMANAGER (PYTHON 3.5) ===")
# Test f-string vs format
rc, out, err = run_cmd("docker exec nodemanager python3 -c \"import sys; print(sys.version); eval('f\\\"{1}\\\"')\"")
print("f-string test returncode:", rc)
print("f-string stderr:", err.strip())

rc, out, err = run_cmd("docker exec nodemanager python3 -c \"print('{}\\t1'.format('127.0.0.1'))\"")
print("format test returncode:", rc, "stdout:", out.strip())

print("\n=== 3. YARN APPLICATION REPORT & LOGS ===")
rc, out, err = run_cmd("docker exec resourcemanager yarn application -list -appStates ALL")
print("YARN applications:\n", out)

print("\n=== 4. YARN NODE REPORT & EFFECTIVE MEMORY ===")
rc, out, err = run_cmd("docker exec resourcemanager yarn node -list -all")
print("YARN nodes:\n", out)

print("\n=== 5. SCHEDULER & CAPACITY CONFIG ===")
rc, out, err = run_cmd("curl -s http://127.0.0.1:8088/ws/v1/cluster/scheduler")
try:
    data = json.loads(out)
    print("Cluster scheduler summary:", json.dumps(data.get("scheduler", {}).get("schedulerInfo", {}).get("queues", {}), indent=2)[:500])
except Exception as e:
    print("Scheduler curl error/raw:", out[:300])

print("\n=== 6. COMPOSE IMAGES & SURVIVABILITY ===")
rc, out, err = run_cmd("docker compose config")
for line in out.splitlines():
    if "image:" in line:
        print("Compose image:", line.strip())

rc, out, err = run_cmd("docker run --rm bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8 python3 --version")
print("Fresh container from committed nodemanager image python3:", out.strip(), err.strip())
rc, out, err = run_cmd("docker run --rm bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 python3 --version")
print("Fresh container from committed namenode image python3:", out.strip(), err.strip())
