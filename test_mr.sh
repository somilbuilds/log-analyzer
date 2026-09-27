#!/bin/bash
set -e

# Copy mapreduce job files to namenode
docker cp batch/mapreduce_job/mapper.py namenode:/tmp/mapper.py
docker cp batch/mapreduce_job/reducer.py namenode:/tmp/reducer.py

# Run hadoop streaming
docker exec namenode hdfs dfs -rm -r -skipTrash /user/data/status_counts || true
docker exec namenode bash -c 'hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -input /user/data/raw_logs -output /user/data/status_counts -mapper "python3 /tmp/mapper.py" -reducer "python3 /tmp/reducer.py" -file /tmp/mapper.py -file /tmp/reducer.py'
