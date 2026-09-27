#!/usr/bin/env python3
"""
Spark Structured Streaming Job — Real-Time Log Analytics
=========================================================

Reads JSON log files produced by the log_replayer, parses them into a
structured schema, computes window aggregates, and writes results to:
  1. HDFS as partitioned Parquet (raw parsed records)
  2. MongoDB (window aggregates, Bloom filter stats, DGIM stats)

Uses file-based streaming: Spark polls a directory for new JSON files.

Usage (submit to Spark cluster):
  spark-submit --master spark://spark-master:7077 \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 \
    streaming_job.py \
    --input-dir /data/stream \
    --hdfs-output hdfs://namenode:9000/user/spark/web_logs \
    --mongo-uri mongodb://mongo:27017/log_analytics \
    --checkpoint-dir hdfs://namenode:9000/user/spark/checkpoints

For local/standalone testing without Spark cluster:
  python streaming_job.py --input-dir /tmp/log_stream \
    --mongo-uri mongodb://localhost:27018/log_analytics \
    --mode local
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

# Add parent directory so we can import bloom_filter, dgim, flajolet_martin
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bloom_filter import BloomFilter
from dgim import DGIM
from flajolet_martin import FlajoletMartin


def run_spark_streaming(args):
    """Run the PySpark Structured Streaming pipeline."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, window, count, sum as spark_sum, lit,
        from_json, expr, current_timestamp,
        date_format, hour, to_date
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType
    )

    # ─── SparkSession ───────────────────────────────────────
    builder = SparkSession.builder.appName("WebLogStreaming")

    if args.mode == "local":
        builder = builder.master("local[2]")
    else:
        builder = builder.master(args.spark_master)

    # Memory-optimized settings
    spark = (builder
             .config("spark.driver.memory", "512m")
             .config("spark.executor.memory", "512m")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.mongodb.output.uri", args.mongo_uri)
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # ─── Schema for JSON files from log_replayer ────────────
    log_schema = StructType([
        StructField("host", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("method", StringType(), True),
        StructField("path", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("bytes", IntegerType(), True),
        StructField("raw", StringType(), True),
        StructField("replay_timestamp", StringType(), True),
    ])

    # ─── Read stream of JSON files ──────────────────────────
    stream_df = (spark
                 .readStream
                 .format("json")
                 .schema(log_schema)
                 .option("maxFilesPerTrigger", 10)
                 .load(args.input_dir))

    # Add processing timestamp and partition columns
    parsed_df = (stream_df
                 .withColumn("process_time", current_timestamp())
                 .withColumn("date", lit(datetime.utcnow().strftime("%Y-%m-%d")))
                 .withColumn("hour_val", lit(datetime.utcnow().hour)))

    # ─── Sink 1: Raw records → HDFS Parquet ─────────────────
    if args.hdfs_output:
        parquet_query = (parsed_df
                         .writeStream
                         .format("parquet")
                         .option("path", args.hdfs_output)
                         .option("checkpointLocation",
                                 args.checkpoint_dir + "/parquet")
                         .partitionBy("date", "hour_val")
                         .outputMode("append")
                         .queryName("hdfs_parquet_sink")
                         .start())

    # ─── Sink 2: Window aggregates → MongoDB ────────────────
    # Use foreachBatch to compute aggregates and write to Mongo
    # plus run Bloom Filter, DGIM, and Flajolet-Martin

    # Initialize stream-mining data structures
    bloom = BloomFilter(expected_elements=50000, fp_rate=0.01)
    dgim_tracker = DGIM(window_size=5000)
    fm_estimator = FlajoletMartin(num_hashes=128, group_size=8)

    # Track exact counts for comparison
    exact_5xx_count = {"value": 0}
    exact_distinct_hosts = {"value": set()}
    window_counter = {"value": 0}

    def process_batch(batch_df, batch_id):
        """Process each micro-batch: compute aggregates and stream-mining stats."""
        if batch_df.rdd.isEmpty():
            return

        rows = batch_df.collect()
        window_counter["value"] += 1
        window_id = window_counter["value"]
        window_time = datetime.utcnow().isoformat()

        # ── Compute aggregates ──────────────────────────────
        total_requests = len(rows)
        status_counts = {}
        host_counts = {}
        path_counts = {}
        total_bytes = 0
        new_hosts = 0
        seen_hosts = 0

        for row in rows:
            host = row["host"] or "unknown"
            status = row["status"] or 0
            path = row["path"] or "/"
            size = row["bytes"] or 0

            # Status code distribution
            status_str = str(status)
            status_counts[status_str] = status_counts.get(status_str, 0) + 1

            # Top hosts
            host_counts[host] = host_counts.get(host, 0) + 1

            # Top paths
            path_counts[path] = path_counts.get(path, 0) + 1

            # Total bytes
            total_bytes += size

            # ── Bloom Filter: new vs seen hosts ─────────────
            was_seen = bloom.add_and_check(host)
            if was_seen:
                seen_hosts += 1
            else:
                new_hosts += 1

            # ── DGIM: track 5xx errors ──────────────────────
            is_5xx = 1 if 500 <= status < 600 else 0
            dgim_tracker.add_bit(is_5xx)
            if is_5xx:
                exact_5xx_count["value"] += 1

            # ── Flajolet-Martin: distinct host estimation ───
            fm_estimator.add(host)
            exact_distinct_hosts["value"].add(host)

        # Sort and get top 10
        top_hosts = sorted(host_counts.items(), key=lambda x: -x[1])[:10]
        top_paths = sorted(path_counts.items(), key=lambda x: -x[1])[:10]

        # Error rate
        error_count = sum(v for k, v in status_counts.items()
                          if k.startswith("4") or k.startswith("5"))
        error_rate = error_count / total_requests if total_requests > 0 else 0

        # ── Write to MongoDB ────────────────────────────────
        try:
            from pymongo import MongoClient
            client = MongoClient(args.mongo_uri)
            db = client.get_default_database()

            # Window aggregates
            db.window_aggregates.insert_one({
                "window_id": window_id,
                "window_time": window_time,
                "batch_id": int(batch_id),
                "total_requests": total_requests,
                "total_bytes": total_bytes,
                "error_rate": round(error_rate, 4),
                "status_distribution": status_counts,
                "top_hosts": [{"host": h, "count": c} for h, c in top_hosts],
                "top_paths": [{"path": p, "count": c} for p, c in top_paths],
            })

            # Bloom filter stats
            db.bloom_stats.insert_one({
                "window_id": window_id,
                "window_time": window_time,
                "new_hosts": new_hosts,
                "seen_hosts": seen_hosts,
                "total_items_in_filter": bloom.items_added,
                "estimated_fp_rate": round(bloom.estimated_fp_rate(), 6),
                "filter_memory_bytes": bloom.memory_usage_bytes(),
            })

            # DGIM stats: approximate vs exact 5xx count
            dgim_approx = dgim_tracker.count()
            db.dgim_stats.insert_one({
                "window_id": window_id,
                "window_time": window_time,
                "dgim_approximate_5xx": dgim_approx,
                "exact_5xx_total": exact_5xx_count["value"],
                "dgim_num_buckets": dgim_tracker.num_buckets(),
                "dgim_window_size": dgim_tracker.window_size,
            })

            # Flajolet-Martin stats: estimated vs exact distinct hosts
            fm_estimate = fm_estimator.estimate()
            exact_distinct = len(exact_distinct_hosts["value"])
            db.fm_stats.insert_one({
                "window_id": window_id,
                "window_time": window_time,
                "fm_estimated_distinct_hosts": fm_estimate,
                "exact_distinct_hosts": exact_distinct,
                "fm_error_pct": round(
                    abs(fm_estimate - exact_distinct) / max(exact_distinct, 1) * 100, 2
                ),
            })

            client.close()
            print(f"[Window {window_id}] {total_requests} requests | "
                  f"Error rate: {error_rate:.2%} | "
                  f"Bloom new/seen: {new_hosts}/{seen_hosts} | "
                  f"DGIM approx 5xx: {dgim_approx} (exact: {exact_5xx_count['value']}) | "
                  f"FM distinct: {fm_estimate} (exact: {exact_distinct})")

        except Exception as e:
            print(f"[Window {window_id}] Error writing to MongoDB: {e}")

    # Start the foreachBatch query
    mongo_query = (parsed_df
                   .writeStream
                   .foreachBatch(process_batch)
                   .option("checkpointLocation",
                           args.checkpoint_dir + "/mongo")
                   .outputMode("append")
                   .queryName("mongo_analytics_sink")
                   .trigger(processingTime="10 seconds")
                   .start())

    print("[StreamingJob] Started. Waiting for data...")
    spark.streams.awaitAnyTermination()


def run_local_mode(args):
    """
    Lightweight local mode: process JSON files without full Spark.
    Useful for testing and when Spark cluster isn't available.
    """
    from pymongo import MongoClient
    import glob

    print("[LocalMode] Starting local processing...")
    print(f"[LocalMode] Input dir: {args.input_dir}")
    print(f"[LocalMode] MongoDB: {args.mongo_uri}")

    bloom = BloomFilter(expected_elements=50000, fp_rate=0.01)
    dgim_tracker = DGIM(window_size=5000)
    fm_estimator = FlajoletMartin(num_hashes=128, group_size=8)

    client = MongoClient(args.mongo_uri)
    db = client.get_default_database()

    # Clear old data
    for coll in ["window_aggregates", "bloom_stats", "dgim_stats", "fm_stats"]:
        db[coll].drop()

    exact_5xx_count = 0
    exact_distinct_hosts = set()
    window_id = 0
    processed_files = set()

    print("[LocalMode] Watching for JSON files... (Ctrl+C to stop)")

    try:
        while True:
            json_files = sorted(glob.glob(os.path.join(args.input_dir, "*.json")))
            new_files = [f for f in json_files if f not in processed_files]

            if not new_files:
                time.sleep(2)
                continue

            # Process in batches of files
            batch_records = []
            for fpath in new_files[:10]:  # Process up to 10 files per batch
                try:
                    with open(fpath, "r") as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                batch_records.append(json.loads(line))
                    processed_files.add(fpath)
                except Exception as e:
                    print(f"[LocalMode] Error reading {fpath}: {e}")
                    processed_files.add(fpath)

            if not batch_records:
                continue

            window_id += 1
            window_time = datetime.utcnow().isoformat()
            total_requests = len(batch_records)

            status_counts = {}
            host_counts = {}
            path_counts = {}
            total_bytes = 0
            new_hosts = 0
            seen_hosts = 0

            for rec in batch_records:
                host = rec.get("host", "unknown")
                status = rec.get("status", 0)
                path = rec.get("path", "/")
                size = rec.get("bytes", 0)

                status_str = str(status)
                status_counts[status_str] = status_counts.get(status_str, 0) + 1
                host_counts[host] = host_counts.get(host, 0) + 1
                path_counts[path] = path_counts.get(path, 0) + 1
                total_bytes += size

                was_seen = bloom.add_and_check(host)
                if was_seen:
                    seen_hosts += 1
                else:
                    new_hosts += 1

                is_5xx = 1 if 500 <= status < 600 else 0
                dgim_tracker.add_bit(is_5xx)
                if is_5xx:
                    exact_5xx_count += 1

                fm_estimator.add(host)
                exact_distinct_hosts.add(host)

            top_hosts = sorted(host_counts.items(), key=lambda x: -x[1])[:10]
            top_paths = sorted(path_counts.items(), key=lambda x: -x[1])[:10]
            error_count = sum(v for k, v in status_counts.items()
                              if k.startswith("4") or k.startswith("5"))
            error_rate = error_count / total_requests if total_requests > 0 else 0

            # Write to MongoDB
            db.window_aggregates.insert_one({
                "window_id": window_id,
                "window_time": window_time,
                "total_requests": total_requests,
                "total_bytes": total_bytes,
                "error_rate": round(error_rate, 4),
                "status_distribution": status_counts,
                "top_hosts": [{"host": h, "count": c} for h, c in top_hosts],
                "top_paths": [{"path": p, "count": c} for p, c in top_paths],
            })

            db.bloom_stats.insert_one({
                "window_id": window_id,
                "window_time": window_time,
                "new_hosts": new_hosts,
                "seen_hosts": seen_hosts,
                "total_items_in_filter": bloom.items_added,
                "estimated_fp_rate": round(bloom.estimated_fp_rate(), 6),
                "filter_memory_bytes": bloom.memory_usage_bytes(),
            })

            dgim_approx = dgim_tracker.count()
            db.dgim_stats.insert_one({
                "window_id": window_id,
                "window_time": window_time,
                "dgim_approximate_5xx": dgim_approx,
                "exact_5xx_total": exact_5xx_count,
                "dgim_num_buckets": dgim_tracker.num_buckets(),
                "dgim_window_size": dgim_tracker.window_size,
            })

            fm_estimate = fm_estimator.estimate()
            exact_distinct = len(exact_distinct_hosts)
            db.fm_stats.insert_one({
                "window_id": window_id,
                "window_time": window_time,
                "fm_estimated_distinct_hosts": fm_estimate,
                "exact_distinct_hosts": exact_distinct,
                "fm_error_pct": round(
                    abs(fm_estimate - exact_distinct) / max(exact_distinct, 1) * 100, 2
                ),
            })

            print(f"[Window {window_id}] {total_requests} reqs | "
                  f"Err: {error_rate:.2%} | "
                  f"Bloom new/seen: {new_hosts}/{seen_hosts} | "
                  f"DGIM 5xx: {dgim_approx} (exact: {exact_5xx_count}) | "
                  f"FM distinct: {fm_estimate} (exact: {exact_distinct})")

            # Also write raw records to a local Parquet-like JSON archive
            archive_dir = os.path.join(args.input_dir, "..", "archive")
            os.makedirs(archive_dir, exist_ok=True)
            archive_file = os.path.join(
                archive_dir,
                f"window_{window_id:06d}.jsonl"
            )
            with open(archive_file, "w") as f:
                for rec in batch_records:
                    f.write(json.dumps(rec) + "\n")

    except KeyboardInterrupt:
        print(f"\n[LocalMode] Stopped. Processed {window_id} windows, "
              f"{len(processed_files)} files.")
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Log Analytics")
    parser.add_argument("--input-dir", required=True,
                        help="Directory with JSON stream files from log_replayer")
    parser.add_argument("--hdfs-output", default="",
                        help="HDFS path for Parquet output (Spark mode only)")
    parser.add_argument("--mongo-uri",
                        default="mongodb://mongo:27017/log_analytics",
                        help="MongoDB connection URI")
    parser.add_argument("--checkpoint-dir",
                        default="hdfs://namenode:9000/user/spark/checkpoints",
                        help="Checkpoint directory for Spark streaming")
    parser.add_argument("--spark-master", default="spark://spark-master:7077",
                        help="Spark master URL")
    parser.add_argument("--mode", choices=["spark", "local"], default="local",
                        help="Run mode: 'spark' for cluster, 'local' for standalone")
    args = parser.parse_args()

    if args.mode == "local":
        run_local_mode(args)
    else:
        run_spark_streaming(args)


if __name__ == "__main__":
    main()
