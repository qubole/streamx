#!/usr/bin/env bash
curl -X POST \
   -H "Accept:application/json" \
   -H "Content-Type:application/json" \
   -d \
'{"name":"camapign-stream",
 "config":
{
"name":"camapign-stream",
"partitioner.class": "io.confluent.connect.hdfs.partitioner.FieldPartitioner",
"partition.field.name": "client_id",
"connector.class":"com.qubole.streamx.s3.S3SinkConnector",
"format.class":"com.qubole.streamx.SourceFormat",
"tasks.max":"1",
"topics":"send.campaign",
"flush.size":"2",
"s3.url":"s3://sailthru-ds-data/tmp/kafka-connect-test/",
"hadoop.conf.dir":"/usr/local/streamx/config/hadoop-conf"
}}' \
 'http://localhost:8083/connectors'