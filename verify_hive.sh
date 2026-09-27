#!/bin/bash
set -e

echo "=== RECREATING CONTAINERS ==="
docker compose stop hive-server hive-metastore hive-metastore-postgresql
docker compose rm -f hive-server hive-metastore hive-metastore-postgresql
docker volume rm bdaproject_hive_postgresql || true
docker rmi bde2020/hive:2.3.2-postgresql-metastore -f || true
docker compose up -d hive-metastore-postgresql
echo "Waiting 10s for Postgres..."
sleep 10
docker compose up -d hive-metastore
echo "Waiting 30s for Metastore to initialize..."
sleep 30
docker compose up -d hive-server

echo "=== WAITING FOR HIVE SERVER ==="
for i in {1..12}; do
    if docker exec hive-server ss -tlnp 2>/dev/null | grep 10000 > /dev/null; then
        echo "Port 10000 is LISTENING!"
        break
    else
        echo "Waiting for port 10000... ($i/12)"
        sleep 10
    fi
done

echo "=== PORT BINDING CHECK ==="
docker exec hive-server ss -tlnp | grep 10000 || echo "Port 10000 not listening yet..."

echo "=== BEELINE CHECK ==="
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" || echo "Beeline failed"

echo "=== RESTART COUNTS ==="
docker inspect hive-metastore --format='hive-metastore restarts: {{.RestartCount}}'
docker inspect hive-server --format='hive-server restarts: {{.RestartCount}}'
