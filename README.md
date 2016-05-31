# StreamX

Forked from the awesome kafka-connect-hdfs(https://github.com/confluentinc/kafka-connect-hdfs). This project will support object store as backend rather than HDFS. Short term, this project will provide a connector that works with s3 and also guarantees exactly-once-delivery.

Clone : `git clone https://github.com/qubole/streamx.git`

Build : `mvn -DskipTests package`

Add Connector to Connect Classpath : 
```export CLASSPATH=$CLASSPATH:`pwd`/target/kafka-connect-hdfs-3.1.0-SNAPSHOT-development/share/java/kafka-connect-hdfs/```

Run connect-distbuted in Kafka : `bin/connect-distibuted.sh config/connect-distributed.sh`

Roadmap
- exactly-once-gurantee for s3
- Support other object stores like Google Cloud Storage and Azure Blob Store
- Currently, data can be written in avro/parquet format. This project will add support for more formats
- Deal with features related to s3, like small-file consolidation
