# Log Analytics BDA Pipeline

A complete end-to-end Big Data Analytics mini-project building a real-time web server log analytics pipeline. This project processes Common Log Format (CLF) web server access logs using a combination of stream processing (Spark Structured Streaming) and batch processing (HDFS, MapReduce, Hive), with real-time outputs shown on a Streamlit dashboard and batch analysis generated via R.

## Architecture 

```text
 +---------+        +---------+        +-----------------+        +---------+
 | Raw CLF |        |   Log   |        | Spark           | =====> | MongoDB | =====> Streamlit
 | Log File| =====> | Replayer| =====> | Structured      |        |         |        Dashboard
 +---------+ Python +---------+  JSON  | Streaming (Py)  | =====> | HDFS    | =====> Hive &
                                       |                 |        | Parquet |        MapReduce
                                       +-----------------+        +---------+
             
                                            |   Stream Mining         |   Batch Analytics
                                            v   Algorithms            v   (R + ggplot2)
                                       +---------+                +---------+
                                       | Bloom   |                | STL     |
                                       | FF, DGIM|                | Anomaly |
                                       | FM      |                | Detect. |
                                       +---------+                +---------+
```

## Prerequisites

1. **Docker & Docker Compose**: The entire infrastructure (Hadoop, Hive, Spark, MongoDB) runs in containers.
2. **Python 3.8+**: For running the log replayer, Spark job natively (or submitting to cluster), and Streamlit dashboard.
3. **Memory**: Allocate at least **8GB (ideally 10GB+) RAM** to Docker/WSL2, as running Spark + Hadoop + Hive + MongoDB simultaneously is resource-intensive. 

## Quick Start Setup

### 1. Generate or Download Dataset
The pipeline expects a CLF web server access log at `data/raw/access_log`.
If you don't have the ClarkNet or NASA log dataset, you can generate a high-volume synthetic log dataset:
```bash
python3 data/raw/generate_dataset.py
```

### 2. Start the Docker Infrastructure
```bash
docker compose up -d
```
This spins up:
- HDFS (NameNode + DataNode) on port `9870`
- YARN Resource Manager on port `8088`
- Hive Metastore & HiveServer2
- Apache Spark (Master + Worker) on port `8080`
- MongoDB on port `27018` (Mapped from `27017` to avoid host conflicts)

Check container health with `docker compose ps` and `docker stats`.

### 3. Run the Pipeline End-to-End
We provide a master script to run the log replayer, Spark streaming, batch mapreduce, and analytics sequentially:
```bash
bash scripts/run_pipeline.sh
```

Alternatively, you can run components individually in multiple terminals:

**Terminal 1: Start Log Replayer (with Anomaly Injection)**
```bash
python3 ingestion/log_replayer.py \
    --input data/raw/access_log \
    --output-dir /tmp/log_stream \
    --speed 5000 \
    --batch-size 200 \
    --inject-anomaly --anomaly-type ddos
```

**Terminal 2: Run Spark Streaming Job**
```bash
python3 streaming/streaming_job.py \
    --input-dir /tmp/log_stream \
    --mongo-uri mongodb://localhost:27018/log_analytics \
    --mode local
```

**Terminal 3: Start Streamlit Dashboard**
```bash
streamlit run dashboard/app.py -- --mongo-uri mongodb://localhost:27018/log_analytics
```

## Stream Mining Algorithms

As requested by standard syllabus topics (MMDS), we implement three approximate stream mining algorithms directly natively in Python applied within Spark structured streaming:

### 1. Bloom Filter (Set Membership)
- **Goal:** Track which IP addresses (hosts) have been "seen before" versus "new" efficiently.
- **Why approximate:** Storing the exact set of millions of unique IP addresses requires unbounded $O(N)$ memory. A Bloom filter uses fixed memory ($O(1)$) (e.g. 100KB bit-array) with a theoretical 0% false-negative and a controllable low false-positive rate.
- **Implementation:** `streaming/bloom_filter.py` uses a bit array and double-hashing (MD5) to mimic $k$ independent hash functions.

### 2. DGIM Algorithm (Counting 1s in a Sliding Window)
- **Goal:** Track an approximate count of 5xx HTTP errors in the last $N$ requests.
- **Why approximate:** Keeping an exact count over a massive sliding window requires storing the timestamp of every event (O(N)). DGIM reduces memory to $O(log^2 N)$ by bucketing older events into exponentially larger power-of-2 sized buckets.
- **Implementation:** `streaming/dgim.py` implements the bucket-merging logic exactly as written in MMDS textbooks.

### 3. Flajolet-Martin (Distinct Counting)
- **Goal:** Estimate the number of unique IP addresses per window.
- **Why approximate:** Flajolet-Martin relies on the max number of trailing zeros resulting from hashing stream elements ($R$). It estimates distinct items as $2^R$ averaging across grouped independent hashes, requiring tiny $O(log N)$ memory compared to exact counting.
- **Implementation:** `streaming/flajolet_martin.py` uses average-of-medians across 128 hashes (16 groups of 8) plus the classic φ ≈ 0.77351 correction. Averaging `2^R` first is dominated by FM's heavy tail (`E[2^R] ≫ n`), which is why a small sample can show ~2× error even with 64 hashes.

## Batch & R Analytics

### Hadoop MapReduce (`batch/mapreduce_job/`)
Standalone Hadoop Streaming Python scripts (`mapper.py` and `reducer.py`) strictly process raw log data inside HDFS to count HTTP status code frequencies. Run via `bash test_mr.sh`.

### R Time-Series Analysis (`analytics/analysis.R`)
Extracts windowed aggregate data from MongoDB to run `stl()` (Seasonal and Trend decomposition using Loess). It highlights and flags anomalous windows where residual variance exceeds 2 standard deviations.

*To run safely without host R dependencies, build the Dockerfile:*
```bash
cd analytics
docker build -t r-analytics .
docker run --rm --network=host r-analytics
```
This saves diagnostic plots (`traffic_timeseries.png`, etc.) locally.
