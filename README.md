StreamX

Forked from the awesome kafka-connect-hdfs(https://github.com/confluentinc/kafka-connect-hdfs). This project will support object store as backend rather than HDFS. Short term, this project will provide a connector that works with s3 and also guarantees exactly-once-delivery.

Roadmap
- exactly-once-gurantee for s3
- Support other object stores like Google Cloud Storage and Azure Blob Store
- Currently, data can be written in avro/parquet format. This project will add support for more formats
- Deal with features related to s3, like small-file consolidation

# Kafka Connect HDFS Connector

kafka-connect-hdfs is a [Kafka Connector](http://kafka.apache.org/090/documentation.html#connect)
for copying data between Kafka and Hadoop HDFS.

# Development

To build a development version you'll need a recent version of Kafka. You can build
kafka-connect-hdfs with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-hdfs
- Issue Tracker: https://github.com/confluentinc/kafka-connect-hdfs/issues


# License

The project is licensed under the Apache 2 license.
