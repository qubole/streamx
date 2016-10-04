# StreamX: Kafka Connect for S3

_Forked from the awesome [kafka-connect-hdfs](https://github.com/confluentinc/kafka-connect-hdfs)_

StreamX is a kafka-connect based connector to copy data from Kafka to Object Stores like Amazon s3, Google Cloud Storage and Azure Blob Store. It focusses on reliable and scalable data copying. It can write the data out in different formats (like parquet, so that it can readily be used by analytical tools) and also in different partitioning requirements.
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
 - Support for storing Hive tables in Qubole's hive metastore (coming soon)
 
##Getting Started:
Pre-req : StreamX is based on Kafka Connect framework, which is part of Kafka project. Kafka Connect is added in Kafka 0.9, hence StreamX can only be used with Kafka version >= 0.9. To download Kafka binaries, check [here](http://kafka.apache.org/downloads.html).

Clone : `git clone https://github.com/qubole/streamx.git`

Branch : For Kafka 0.9, use 2.x branch. For Kafka 0.10 and above, use master branch.

Build : `mvn -DskipTests package`

Once the build succeeds, StreamX packages all required jars under target/streamx-0.1.0-SNAPSHOT-development/share/java/streamx/* in StreamX repo. This directory needs to be in classpath.

Add Connector to Kafka Connect Classpath : 
```export CLASSPATH=$CLASSPATH:`pwd`/target/streamx-0.1.0-SNAPSHOT-development/share/java/streamx/*```


#### Start Kafka Connect

In Kafka, change the following in config/connect-distibuted.properties

*bootstrap.servers* to Kafka end-point (ex: localhost:9092)

To copy data from Kafka as-is without any changes, use ByteArrayConverter. Change key.converter and value.converter to the following.

key.converter=com.qubole.streamx.ByteArrayConverter
value.converter=com.qubole.streamx.ByteArrayConverter

Run Kafka Connect `bin/connect-distibuted.sh config/connect-distributed.properties`

We have started the Kafka Connect framework and the S3 Connector is added to classpath. Kafka Connect framework starts a REST server (rest.port property in connect-distributed.properties) listening for Connect Job requests. The copy job can be submitted by hitting the REST end-point using curl or any REST clients.

For example, to submit a copy job from Kafka to S3

```
curl -i -X POST \
   -H "Accept:application/json" \
   -H "Content-Type:application/json" \
   -d \
'{"name":"clickstream",
 "config":
{
"name":"clickstream",
"connector.class":"com.qubole.streamx.s3.S3SinkConnector",
"format.class":"com.qubole.streamx.SourceFormat",
"tasks.max":"1",
"topics":"adclicks",
"flush.size":"2",
"s3.url":"s3://streamx/demo",
"hadoop.conf.dir":"/Users/pseluka/src/streamx/hadoop-conf"
}}' \
 'http://localhost:8083/connectors'
```

- Uses *S3SinkConnector*
- Uses *SourceFormat*, which copies the data as-is (Note that this needs to be used with ByteArrayConverter)
- *tasks.max* refers to number of tasks that copies the data
- a new file is written after *flush.size* number of messages
- S3 Configuration
It uses the hadoop file system implementation (s3a/s3n) to write to s3. The connect job has a configuration called *hadoop.conf.dir* and this needs the directory where core-site.xml and other hadoop configuration resides. StreamX packages the hadoop dependencies, so it need not have hadoop project/jars in its classpath. So, create a directory containing hadoop config files like core-site.xml, hdfs-site.xml and provide the location of this directory in hadoop.conf.dir while submitting copy job.

For quick reference, 

##### if you are using S3NativeFileSystem
```
<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <description>AWS access key ID</description>
</property>

<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <description>AWS secret key</description>
</property>
```
##### if you are using S3AFileSystem
```
<property>
  <name>fs.s3a.access.key</name>
  <description>AWS access key ID. Omit for Role-based authentication.</description>
</property>

<property>
  <name>fs.s3a.secret.key</name>
  <description>AWS secret key. Omit for Role-based authentication.</description>
</property>
```

You have submitted the job, check S3 for output files. For the above copy job, it will create
s3://streamx/demo/topics/adclicks/partition=x/files.xyz

Note that, a single copy job could consume from multiple topics and writes to topic specific directory.

To delete a Connect job,
```
curl -i -X DELETE \
   -H "Accept:application/json" \
   -H "Content-Type:application/json" \
 'http://localhost:8083/connectors/clickstream'
```

To list all Connect jobs,
```
curl -i -X GET \
   -H "Accept:application/json" \
   -H "Content-Type:application/json" \
 'http://localhost:8083/connectors'
```

**Restarting Connect jobs** : All Connect jobs are stored in a Kafka Queue. So, restarting the Kafka Connect will restart all the connectors submitted to it.

##Roadmap
- Support other object stores like Google Cloud Storage and Azure Blob Store
- Currently, data can be written in avro/parquet format. This project will add support for more formats
- Deal with features related to s3, like small-file consolidation
