#!/usr/bin/env python3
"""
Populate all 25 HDFS task output directories with authentic MapReduce results.
Runs mapper | sort | reducer locally, then uploads the output to HDFS.

Each task gets its OWN unique output directory — no sharing.
"""
import os
import subprocess
import sys
import time

# Derive project root from THIS script's location
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from api.hadoop import TASK_CATALOG

raw = os.path.join(ROOT, "data", "raw", "access_log")
if not os.path.isfile(raw):
    print(f"ERROR: Dataset not found at {raw}")
    print("Run scripts/download_clarknet.sh first.")
    sys.exit(1)

os.makedirs("/tmp/hdfs_cache", exist_ok=True)

# Build per-task jobs — each task gets its own unique run
tasks_to_run = []
for t in TASK_CATALOG:
    tasks_to_run.append({
        "id": t["id"],
        "mapper": t["mapper"],
        "reducer": t["reducer"],
        "output": t["output"],
        "name": t["name"],
    })

print(f"Processing {len(tasks_to_run)} tasks from TASK_CATALOG...")
print(f"Dataset: {raw}")
print()

success_count = 0
skip_count = 0
fail_count = 0

for task in tasks_to_run:
    task_id = task["id"]
    output_path = task["output"]
    mapper = task["mapper"]
    reducer = task["reducer"]

    # Check if output already exists in HDFS
    check = subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-test", "-e", f"{output_path}/part-00000"],
        capture_output=True
    )
    if check.returncode == 0:
        print(f"  ✓ {task_id:25s} — already in HDFS at {output_path}")
        skip_count += 1
        continue

    t0 = time.time()
    out_file = f"/tmp/hdfs_cache/{task_id}.txt"
    map_script = os.path.join(ROOT, "batch", "mapreduce_job", mapper)
    red_script = os.path.join(ROOT, "batch", "mapreduce_job", reducer)

    # Verify scripts exist
    if not os.path.isfile(map_script):
        print(f"  ✗ {task_id:25s} — mapper not found: {map_script}")
        fail_count += 1
        continue
    if not os.path.isfile(red_script):
        print(f"  ✗ {task_id:25s} — reducer not found: {red_script}")
        fail_count += 1
        continue

    print(f"  ⟳ {task_id:25s} — {mapper} → {reducer} ...", end=" ", flush=True)

    try:
        # Run pipeline: mapper | sort | reducer
        with open(raw, "rb") as f_in:
            p1 = subprocess.Popen(["python3", map_script], stdin=f_in, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        p2 = subprocess.Popen(["sort"], stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p1.stdout.close()

        with open(out_file, "wb") as f_out:
            p3 = subprocess.Popen(["python3", red_script], stdin=p2.stdout, stdout=f_out, stderr=subprocess.PIPE)
            p2.stdout.close()
            _, p3_err = p3.communicate(timeout=300)

        # Check exit codes
        p1.wait(timeout=10)
        p2.wait(timeout=10)

        if p1.returncode != 0:
            print(f"FAILED (mapper exit={p1.returncode})")
            fail_count += 1
            continue
        if p2.returncode != 0:
            print(f"FAILED (sort exit={p2.returncode})")
            fail_count += 1
            continue
        if p3.returncode != 0:
            print(f"FAILED (reducer exit={p3.returncode})")
            if p3_err:
                print(f"         stderr: {p3_err.decode('utf-8', errors='replace')[:200]}")
            fail_count += 1
            continue

        # Verify output file is non-empty
        if not os.path.isfile(out_file) or os.path.getsize(out_file) == 0:
            print(f"FAILED (empty output)")
            fail_count += 1
            continue

        elapsed = time.time() - t0

        # Upload to HDFS
        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", output_path],
            check=True, capture_output=True
        )
        subprocess.run(
            ["docker", "cp", out_file, f"namenode:/tmp/{task_id}.txt"],
            check=True, capture_output=True
        )
        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-D",
             "dfs.client.use.datanode.hostname=false", "-put", "-f",
             f"/tmp/{task_id}.txt", f"{output_path}/part-00000"],
            check=True, capture_output=True
        )
        subprocess.run(
            ["docker", "exec", "namenode", "rm", "-f", f"/tmp/{task_id}.txt"],
            check=False, capture_output=True
        )

        # Clean up local temp file
        if os.path.isfile(out_file):
            os.remove(out_file)

        # Verify upload
        verify = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-test", "-e", f"{output_path}/part-00000"],
            capture_output=True
        )
        if verify.returncode != 0:
            print(f"FAILED (HDFS upload verification failed)")
            fail_count += 1
            continue

        print(f"OK ({elapsed:.1f}s)")
        success_count += 1

    except subprocess.TimeoutExpired:
        print(f"FAILED (timeout)")
        fail_count += 1
        # Kill any remaining processes
        for p in [p1, p2, p3]:
            try:
                p.kill()
            except Exception:
                pass
    except subprocess.CalledProcessError as e:
        print(f"FAILED ({e})")
        fail_count += 1
    except Exception as e:
        print(f"FAILED ({e})")
        fail_count += 1

print()
print(f"{'='*60}")
print(f"Results: {success_count} generated, {skip_count} skipped (already exist), {fail_count} failed")
total_expected = len(tasks_to_run)
total_ok = success_count + skip_count
if total_ok == total_expected:
    print(f"SUCCESS: All {total_expected} tasks now have outputs in HDFS!")
else:
    print(f"WARNING: {total_expected - total_ok} tasks are missing outputs.")
    sys.exit(1)
