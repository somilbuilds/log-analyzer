#!/bin/bash
# Verification script for the BDA pipeline
set -e

echo "=== 1. MongoDB Collection Counts ==="
docker exec mongo mongosh log_analytics --quiet --eval '
  db.getCollectionNames().forEach(function(c) {
    print(c + ": " + db.getCollection(c).countDocuments() + " docs");
  });
'

echo ""
echo "=== 2. Sample Window Aggregate ==="
docker exec mongo mongosh log_analytics --quiet --eval '
  printjson(db.window_aggregates.findOne({}, {_id:0, window_id:1, total_requests:1, error_rate:1}));
'

echo ""
echo "=== 3. Sample DGIM Stats ==="
docker exec mongo mongosh log_analytics --quiet --eval '
  printjson(db.dgim_stats.findOne({}, {_id:0}));
'

echo ""
echo "=== 4. Sample Bloom Stats ==="
docker exec mongo mongosh log_analytics --quiet --eval '
  printjson(db.bloom_stats.findOne({}, {_id:0}));
'

echo ""
echo "=== 5. Sample FM Stats ==="
docker exec mongo mongosh log_analytics --quiet --eval '
  printjson(db.fm_stats.findOne({}, {_id:0}));
'

echo ""
echo "=== 6. HDFS ls / ==="
docker exec namenode hdfs dfs -ls /

echo ""
echo "=== 7. Upload raw log to HDFS ==="
# Copy log file into namenode container first
docker cp "$(cd "$(dirname "$0")/.." && pwd)/data/raw/access_log" namenode:/tmp/access_log
docker exec namenode hdfs dfs -mkdir -p /user/data/raw_logs
docker exec namenode hdfs dfs -put -f /tmp/access_log /user/data/raw_logs/
echo "Uploaded. Verifying:"
docker exec namenode hdfs dfs -ls /user/data/raw_logs/

echo ""
echo "=== 8. Hive Status ==="
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" 2>&1 || echo "Hive not ready yet"

echo ""
echo "=== DONE ==="
