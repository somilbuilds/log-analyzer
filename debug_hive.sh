#!/bin/bash
set -x

docker logs hive-server | head -n 20
echo "----"
docker logs hive-server | tail -n 20
