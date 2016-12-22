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

In Kafka, change the following in config/connect-distibuted.properties or config/connect-standalone.properties depending on what mode you want to use.

bootstrap.servers=set Kafka end-point (ex: localhost:9092)
key.converter=com.qubole.streamx.ByteArrayConverter
value.converter=com.qubole.streamx.ByteArrayConverter
Use ByteArrayConverter to copy data from Kafka as-is without any changes. (copy JSON/CSV)

##### Run Kafka Connect in Standalone mode
Set *s3.url* and *hadoop-conf* in StreamX config/quickstart-s3.properties. StreamX packages hadoop-conf directory at config/hadoop-conf for ease-of-use. Set s3 access and secret keys in config/hadoop-conf/hdfs-site.xml

In Kafka, run
`bin/connect-standalone etc/kafka/connect-standalone.properties /path/to/streamx/config/quickstart-s3.properties`

You are done. Check s3 for ingested data !

##### Run Kafka Connect in distributed mode
`bin/connect-distributed.sh config/connect-distributed.properties`

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
It uses the hadoop file system implementation (s3a/s3n) to write to s3. The connect job has a configuration called *hadoop.conf.dir* and this needs the directory where hdfs-site.xml and other hadoop configuration resides. StreamX packages the hadoop dependencies, so it need not have hadoop project/jars in its classpath. So, create a directory containing hadoop config files like core-site.xml, hdfs-site.xml and provide the location of this directory in *hadoop.conf.dir* while submitting copy job. (StreamX provides a default hadoop-conf directory under config/hadoop-conf. Set your s3 access key, secret key there and provide full path in *hadoop.conf.dir*)

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

**Docker**
Streamx supports Docker, but only in distributed mode
To build your image,
```
docker build -t qubole/streamx .
```
When you run your container, you can override all the properties in connect-distributed.properties file with env vars. env_vars will be of format CONNECT_BOOTSTRAP_SERVERS corresponding to bootstrap.servers. The convention is to prefix env vars with CONNECT.
Example of how to run your container,
```
docker run -d -p 8083:8083 --env CONNECT_BOOTSTRAP_SERVERS=public_dns:9092 --env CONNECT_AWS_ACCESS_KEY=youracesskey --env CONNECT_AWS_SECRET_KEY=yoursecretkey qubole/streamx
```
You can also use Avro/Parquet format. Example:
```
docker run -d -p 8083:8083 --env CONNECT_BOOTSTRAP_SERVERS=public_dns:9092 --env CONNECT_AWS_ACCESS_KEY=youracesskey --env CONNECT_AWS_SECRET_KEY=yoursecretkey  --env CONNECT_KEY_CONVERTER=io.confluent.connect.avro.AvroConverter --env CONNECT_VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter --env CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL=http://your.schema.registry.com:8081 --env CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=http://your.schema.registry.com:8081 qubole/streamx

```

##Roadmap
- Support other object stores like Google Cloud Storage and Azure Blob Store
- Currently, data can be written in avro/parquet format. This project will add support for more formats
- Deal with features related to s3, like small-file consolidation
