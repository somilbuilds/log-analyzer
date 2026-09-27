#!/bin/bash
set -e

echo "=== HIVE SERVER DATABASES ==="
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" 2>/dev/null || echo "Hive Beeline failed"

echo -e "\n=== SPARK SQL METASTORE CONNECTION ==="
docker exec spark-master /spark/bin/spark-sql -e "show databases;" || echo "Spark SQL failed"

echo -e "\n=== HDFS PATH VERIFICATION ==="
docker exec namenode hdfs dfs -mkdir -p /user/spark/web_logs
docker exec namenode hdfs dfs -ls /user/spark/

echo -e "\n=== MONGODB DB STATS ==="
docker exec mongo mongosh log_analytics --quiet --eval "db.stats()"
