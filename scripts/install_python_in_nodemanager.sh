#!/usr/bin/env bash
set -ex

echo "=== Configuring archive.debian.org in nodemanager ==="
docker exec nodemanager sh -c 'printf "deb [check-valid-until=no] http://archive.debian.org/debian stretch main\n" > /etc/apt/sources.list'
docker exec nodemanager sh -c 'printf "Acquire::Check-Valid-Until \"false\";\n" > /etc/apt/apt.conf.d/99no-check-valid-until'

echo "=== Updating apt cache ==="
docker exec nodemanager apt-get update

echo "=== Installing python3 ==="
docker exec nodemanager apt-get install --no-install-recommends -y python3

echo "=== Installing python3 in namenode ==="
docker exec namenode sh -c 'printf "deb [check-valid-until=no] http://archive.debian.org/debian stretch main\n" > /etc/apt/sources.list'
docker exec namenode sh -c 'printf "Acquire::Check-Valid-Until \"false\";\n" > /etc/apt/apt.conf.d/99no-check-valid-until'
docker exec namenode apt-get update
docker exec namenode apt-get install --no-install-recommends -y python3
docker exec namenode python3 --version
