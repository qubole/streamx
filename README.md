# StreamX: Kafka Connect for S3

_Forked from the awesome [kafka-connect-hdfs](https://github.com/confluentinc/kafka-connect-hdfs)_

StreamX is a kafka-connect based connector to copy data from Kafka to Object Stores like Amazon s3, Google Cloud Storage and Azure Blob Store. It focusses on reliable and scalable data copying. One design goal is to write the data in format (like parquet), so that it can readily be used by analytical tools.

##Features :

StreamX inherits rich set of features from kafka-connect-hdfs. 
 - Support for writing data in Avro and Parquet formats.
 - Provides Hive Integration where the connector creates patitioned hive table and periodically does add partitions once it writes a new partition to s3
 - Pluggable partitioner : 
  - default partitioner : Each Kafka partition will have its data copied under a partition specific directory
  - time based partitioner : Ability to write data on hourly basis
  - field based partitioner : Ability to use a field in the record as custom partitioner
  
In addition to these, we have made changes to the following to make it work efficiently with s3
 - Exactly-once guarantee using WAL
 - Direct output to S3 (Avoid writing to temporary file and renaming it. In S3, rename is copy + delete which is expensive)
 - Support for storing Hive tables in Qubole's hive metastore (coming soon)
 
##Getting Started:
Clone : `git clone https://github.com/qubole/streamx.git`

Build : `mvn -DskipTests package`

Add Connector to Connect Classpath : 
```export CLASSPATH=$CLASSPATH:`pwd`/target/streamx-0.1.0-SNAPSHOT-development/share/java/streamx/```

Run connect-distbuted in Kafka : `bin/connect-distibuted.sh config/connect-distributed.sh`

### S3 Configuration
It uses the hadoop file system implementation (s3a/s3n) to write to s3. The connect job has a configuration called _hadoop.conf.dir_ and this needs the directory where core-site.xml and other hadoop configuration resides. StreamX packages the hadoop dependencies, so it need not have hadoop project/jars in its classpath. 

For quick reference, these properties must be present in your core-site.xml. This 
- fs.AbstractFileSystem.s3.impl=org.apache.hadoop.fs.s3a.S3A
- fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
- fs.s3a.access.key=xxxxx
- fs.s3a.secret.key=xxxxx

### Sample Job

You need to change all the values that has "<..>"
```
{"name":"twitter connector",
"config":{
"name":"twitter connector",
"connector.class":"com.qubole.streamx.s3.S3SinkConnector",
"tasks.max":"1",
"flush.size":"100000",
"s3.url":"<S3 location>",
"wal.class":"com.qubole.streamx.s3.wal.DBWAL",
"db.connection.url":"<jdbc:mysql://localhost:3306/kafka>",
"db.user":"<username_required>",
"db.password":"<password_required>",
"hadoop.conf.dir":"<directory where hadoop conf files are stored. Example /usr/lib/hadoop2/etc/hadoop/>",
"topics":"twitter1p"}}
```

##Roadmap
- Support other object stores like Google Cloud Storage and Azure Blob Store
- Currently, data can be written in avro/parquet format. This project will add support for more formats
- Deal with features related to s3, like small-file consolidation
