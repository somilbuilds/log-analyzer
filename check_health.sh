#!/bin/bash
set -e

echo "=== DOCKER COMPOSE PS ==="
docker compose ps

echo -e "\n=== CONTAINER RESTARTS ==="
for c in $(docker compose ps -q); do
    docker inspect $c --format='{{.Name}}: {{.RestartCount}}'
done

echo -e "\n=== SPARK MASTER UI ==="
curl -s http://localhost:8080 | grep 'Alive Workers' || echo "Spark master unreachable"

echo -e "\n=== HIVE SERVER LOGS (Tail) ==="
docker logs hive-server | tail -n 10
