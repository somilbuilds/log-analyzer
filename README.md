# Log Analytics BDA Pipeline

Real-time analytics over the **ClarkNet-HTTP** traces (Internet Traffic Archive, 1995): replay Common Log Format lines, run Bloom / DGIM / Flajolet–Martin on the stream, store window stats in MongoDB, and watch them on a React dashboard.

```text
 ClarkNet CLF  ->  Log replayer  ->  Stream job (local or Spark)
                                      |                 |
                                      v                 v
                                   MongoDB            HDFS / Hive / MapReduce
                                      |
                                      v
                               FastAPI  ->  React UI
```

## Prerequisites

- Docker Compose (Hadoop, Hive, Spark, MongoDB)
- Python 3.10+ and Node 18+
- About 8GB RAM for the full container stack

## Quick start

```bash
cd ~/projects/bdaproject
chmod +x start.sh stop.sh scripts/*.sh
./start.sh
```

That script:

1. Downloads the real ClarkNet traces (both weeks) into `data/raw/access_log` if they are missing
2. Starts the Docker stack
3. Starts the API (`:8000`) and dashboard (`:3000`)
4. Starts the live replay
5. Opens the browser

Stop local processes with `./stop.sh`. The Docker stack stays up until you run `docker compose down`.

## Dataset

ClarkNet is **not** generated. `scripts/download_clarknet.sh` pulls:

- https://ita.ee.lbl.gov/traces/clarknet_access_log_Aug28.gz
- https://ita.ee.lbl.gov/traces/clarknet_access_log_Sep4.gz

together ~3.3 million CLF lines (Aug 28–Sep 10 1995). The uncompressed log is gitignored.

## Useful URLs

| Service | URL |
|---|---|
| Dashboard | http://localhost:3000 |
| API docs | http://localhost:8000/docs |
| HDFS | http://localhost:9870 |
| YARN | http://localhost:8088 |
| Spark | http://localhost:8080 |
| MongoDB | `mongodb://localhost:27018/log_analytics` |

## Stream mining

- **Bloom filter** — new vs already-seen hosts in constant memory
- **DGIM** — approximate 5xx count in a sliding window
- **Flajolet–Martin** — distinct host estimate vs exact set

## Batch path

```bash
bash scripts/run_pipeline.sh    # replay + stream + MapReduce + Hive + R
bash scripts/verify.sh          # collection counts and HDFS upload
```

MapReduce scripts live in `batch/mapreduce_job/`. R plots: `analytics/analysis.R` (or the `analytics/Dockerfile`).
