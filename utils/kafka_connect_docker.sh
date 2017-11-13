#!/usr/bin/env bash
docker run --rm -p 8083:8083/tcp -p 4001:4001 \
--name=kafka-s3 \
--net=containers_sandbox \
--env KAFKA_DEBUG=true \
--env JAVA_DEBUG_PORT=4001 \
--env DEBUG_SUSPEND_FLAG="n" \
--env CONNECT_BOOTSTRAP_SERVERS=kafka:9092 \
--env CONNECT_AWS_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
--env CONNECT_AWS_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" \
--env CONNECT_CLUSTER_ON_ROLES=True \
qubole/streamx