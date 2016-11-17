HDFS Connector
==============

The HDFS connector allows you to export data from Kafka topics to HDFS files in a variety of formats
and integrates with Hive to make data immediately available for querying with HiveQL.

The connector periodically polls data from Kafka and writes them to HDFS. The data from each Kafka
topic is partitioned by the provided partitioner and divided into chucks. Each chunk of data is
represented as an HDFS file with topic, kafka partition, start and end offsets of this data chuck
in the filename. If no partitioner is specified in the configuration, the default partitioner which
preserves the Kafka partitioning is used. The size of each data chunk is determined by the number of
records written to HDFS, the time written to HDFS and schema compatibility.

The HDFS connector integrates with Hive and when it is enabled, the connector automatically creates
an external Hive partitioned table for each Kafka topic and updates the table according to the
available data in HDFS.

Quickstart
----------
In this Quickstart, we use the HDFS connector to export data produced by the Avro console producer
to HDFS.

Start Zookeeper, Kafka and SchemaRegistry if you haven't done so. The instructions on how to start
these services are available at the Confluent Platform QuickStart. You also need to have Hadoop
running locally or remotely and make sure that you know the HDFS url. For Hive integration, you
need to have Hive installed and to know the metastore thrift uri.

This Quickstart assumes that you started the required services with the default configurations and
you should make necessary changes according to the actual configurations used.

.. note:: You need to make sure the connector user have write access to the directories
   specified in ``topics.dir`` and ``logs.dir``. The default value of ``topics.dir`` is
   ``/topics`` and the default value of ``logs.dir`` is ``/logs``, if you don't specify the two
   configurations, make sure that the connector user has write access to ``/topics`` and ``/logs``.
   You may need to create ``/topics`` and ``/logs`` before running the connector as the connector
   usually don't have write access to ``/``.

Also, this Quickstart assumes that security is not configured for HDFS and Hive metastore,
please make the necessary configurations change following `Secure HDFS and Hive Metastore`_
section.

First, start the Avro console producer::

  $ ./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic test_hdfs \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

Then in the console producer, type in::

  {"f1": "value1"}
  {"f1": "value2"}
  {"f1": "value3"}

The three records entered are published to the Kafka topic ``test_hdfs`` in Avro format.

Before starting the connector, please make sure that the configurations in
``etc/kafka-connect-hdfs/quickstart-hdfs.properties`` are properly set to your configurations of
Hadoop, e.g. ``hdfs.url`` points to the proper HDFS and using FQDN in the host. Then run the
following command to start Kafka connect with the HDFS connector::


  $ ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties \
  etc/kafka-connect-hdfs/quickstart-hdfs.properties

You should see that the process starts up and logs some messages, and then exports data from Kafka
to HDFS. Once the connector finishes ingesting data to HDFS, check that the data is available
in HDFS::

  $ hadoop fs -ls /topics/test_hdfs/partition=0

You should see a file with name ``/topics/test_hdfs/partition=0/test_hdfs+0+0000000000+0000000002.avro``
The file name is encoded as ``topic+kafkaPartition+startOffset+endOffset.format``.

You can use ``avro-tools-1.7.7.jar``
(available in `Apache mirrors <http://mirror.metrocast.net/apache/avro/avro-1.7.7/java/avro-tools-1.7.7.jar>`_)
to extract the content of the file. Run ``avro-tools`` directly on Hadoop as::

  $ hadoop jar avro-tools-1.7.7.jar tojson \
  /topics/test_hdfs/partition=0/test_hdfs+0+0000000000+0000000002.avro

or, if you experience issues, first copy the avro file from HDFS to the local filesystem and try again with java::

  $ hadoop fs -copyToLocal /topics/test_hdfs/partition=0/test_hdfs+0+0000000000+0000000002.avro \
  /tmp/test_hdfs+0+0000000000+0000000002.avro

  $ java -jar avro-tools-1.7.7.jar tojson /tmp/test_hdfs+0+0000000000+0000000002.avro

You should see the following output::

  {"f1":"value1"}
  {"f1":"value2"}
  {"f1":"value3"}


.. note:: If you want to run the Quickstart with Hive integration, before starting the connector,
   you need to add the following configurations to
   ``etc/kafka-connect-hdfs/quickstart-hdfs.properties``::

      hive.integration=true
      hive.metastore.uris=thrift uri to your Hive metastore
      schema.compatibility=BACKWARD

   After the connector finishes ingesting data to HDFS, you can use Hive to check the data::

      $hive>SELECT * FROM test_hdfs;

.. note:: If you leave the ``hive.metastore.uris`` empty, an embedded Hive metastore will be
   created in the directory the connector is started. You need to start Hive in that specific
   directory to query the data.

Features
--------
The HDFS connector offers a bunch of features:

* **Exactly Once Delivery**: The connector uses a write ahead log to ensure each record exports
  to HDFS exactly once. Also, the connector manages offsets commit by encoding the Kafka offset
  information into the file so that the we can start from the last committed offsets in case of
  failures and task restarts.

* **Extensible Data Format**: Out of the box, the connector supports writing data to HDFS in Avro
  and Parquet format. Also, you can write other formats to HDFS by extending the ``Format`` class.

* **Hive Integration**: The connector supports Hive integration out of the box, and when it is
  enabled, the connector automatically creates a Hive external partitioned table for each topic
  exported to HDFS.

* **Schema Evolution**: The connector supports schema evolution and different schema compatibility
  levels. When the connector observes a schema change, it projects to the proper schema according
  to the ``schema.compatibility`` configuration. Hive integration is supported if ``BACKWARD``,
  ``FORWARD`` and ``FULL`` is specified for ``schema.compatibility`` and Hive tables have the
  table schema that are able to query the whole data under a topic written with different schemas.

* **Secure HDFS and Hive Metastore Support**: The connector supports Kerberos authentication and
  thus works with secure HDFS and Hive metastore.

* **Pluggable Partitioner**: The connector supports default partitioner, field partitioner, and
  time based partitioner including daily and hourly partitioner out of the box. You can implement
  your own partitioner by extending the ``Partitioner`` class. Plus, you can customize time based
  partitioner by extending the ``TimeBasedPartitioner`` class.

Configuration
-------------
This section gives example configurations that cover common scenarios, then provides an exhaustive
description of the available configuration options.

Example
~~~~~~~
Here is the content of ``etc/kafka-connect-hdfs/quickstart-hdfs.properties``::

  name=hdfs-sink
  connector.class=io.confluent.connect.hdfs.HdfsSinkConnector
  tasks.max=1
  topics=test_hdfs
  hdfs.url=hdfs://localhost:9000
  flush.size=3

The first few settings are common settings you'll specify for all connectors. The ``topics``
specifies the topics we want to export data from, in this case ``test_hdfs``. The ``hdfs.url``
specifies the HDFS we are writing data to and you should set this according to your configuration.
The ``flush.size`` specifies the number of records the connector need to write before invoking file
commits.

.. note:: For HA HDFS deployments you will need to include ``hadoop.conf.dir``, setting it to a directory which includes hdfs-site.xml. Once hdfs-site.xml is in place and ``hadoop.conf.dir`` has been set, ``hdfs.url`` may be set to the namenodes nameservice id. i.e. 'nameservice1' . 


Format and Partitioner
~~~~~~~~~~~~~~~~~~~~~~
You need to specify the ``format.class`` and ``partitioner.class`` if you want to write other
formats to HDFS or use other partitioners. The following example configurations demonstrates how to
write Parquet format and use hourly partitioner::

  format.class=io.confluent.connect.hdfs.parquet.ParquetFormat
  partitioner.class=io.confluent.connect.hdfs.partitioner.HourlyPartitioner

.. note:: If you want ot use the field partitioner, you need to specify the ``partition.field.name``
   configuration as well to specify the field name of the record.

Hive Integration
~~~~~~~~~~~~~~~~
At minimum, you need to specify ``hive.integration``, ``hive.metastore.uris`` and
``schema.compatibility`` when integrating Hive. Here is an example configuration::

  hive.integration=true
  hive.metastore.uris=thrift://localhost:9083 # FQDN for the host part
  schema.compatibility=BACKWARD

You should adjust the ``hive.metastore.uris`` according to your Hive configurations.

.. note:: If you don't specify the ``hive.metastore.uris``, the connector will use a local metastore
   with Derby in the directory running the connector. You need to run Hive in this directory
   in order to see the Hive metadata change.

.. note:: As connector tasks are long running, the connections to Hive metastore are kept open
   until tasks are stopped. In the default Hive configuration, reconnecting to Hive metastore creates
   a new connection. When the number of tasks is large, it is possible that the retries can cause
   the number of open connections to exceed the max allowed connections in the operating system.
   Thus it is recommended to set ``hcatalog.hive.client.cache.disabled`` to ``true`` in ``hive.xml``.

Also, to support schema evolution, the ``schema.compatibility`` to be ``BACKWARD``, ``FORWARD`` and
``FULL``. This ensures that Hive can query the data written to HDFS with different schemas using the
latest Hive table schema. Please find more information on schema compatibility in the
`Schema Evolution`_ section.

Secure HDFS and Hive Metastore
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To work with secure HDFS and Hive metastore, you need to specify ``hdfs.authentication.kerberos``,
``connect.hdfs.principal``, ``connect.keytab``, ``hdfs.namenode.principal``::

  hdfs.authentication.kerberos=true
  connect.hdfs.principal=connect-hdfs/_HOST@YOUR-REALM.COM
  connect.hdfs.keytab=path to the connector keytab
  hdfs.namenode.principal=namenode principal

You need to create the Kafka connect principals and keytab files via Kerboros and distribute the
keytab file to all hosts that running the connector and ensures that only the connector user
has read access to the keytab file.

.. note:: When security is enabled, you need to use FQDN for the host part of
   ``hdfs.url`` and ``hive.metastore.uris``.

.. note:: Currently, the connector requires that the principal and the keytab path to be the same
   on all the hosts running the connector. The host part of the ``hdfs.namenode.prinicipal`` needs
   to be the actual FQDN of the Namenode host instead of the ``_HOST`` placeholder.

Schema Evolution
----------------
The HDFS connector supports schema evolution and reacts to schema changes of data according to the
``schema.compatibility`` configuration. In this section, we will explain how the
connector reacts to schema evolution under different values of ``schema.compatibility``. The
``schema.compatibility`` can be set to ``NONE``, ``BACKWARD``, ``FORWARD`` and ``FULL``, which means
NO compatibility, BACKWARD compatibility, FORWARD compatibility and FULL compatibility respectively.

* **NO Compatibility**: By default, the ``schema.compatibility`` is set to ``NONE``. In this case,
  the connector ensures that each file written to HDFS has the proper schema. When the connector
  observes a schema change in data, it commits the current set of files for the affected topic
  partitions and writes the data with new schema in new files.

* **BACKWARD Compatibility**: If a schema is evolved in a backward compatible way, we can always
  use the latest schema to query all the data uniformly. For example, removing fields is backward
  compatible change to a schema, since when we encounter records written with the old schema that
  contain these fields we can just ignore them. Adding a field with a default value is also backward
  compatible.

  If ``BACKWARD`` is specified in the ``schema.compatibility``, the connector keeps track
  of the latest schema used in writing data to HDFS, and if a data record with a schema version
  larger than current latest schema arrives, the connector commits the current set of files
  and writes the data record with new schema to new files. For data records arriving at a later time
  with schema of an earlier version, the connector projects the data record to the latest schema
  before writing to the same set of files in HDFS.

* **FORWARD Compatibility**: If a schema is evolved in a forward compatible way, we can always
  use the oldest schema to query all the data uniformly. Removing a field that had a default value
  is forward compatible, since the old schema will use the default value when the field is missing.

  If ``FORWARD`` is specified in the ``schema.compatibility``, the connector projects the data to
  the oldest schema before writing to the same set of files in HDFS.

* **Full Compatibility**: Full compatibility means that old data can be read with the new schema
  and new data can also be read with the old schema.

  If ``FULL`` is specified in the ``schema.compatibility``, the connector performs the same action
  as ``BACKWARD``.

If Hive integration is enabled, we need to specify the ``schema.compatibility`` to be ``BACKWARD``,
``FORWARD`` or ``FULL``. This ensures that the Hive table schema is able to query all the data under
a topic written with different schemas. If the ``schema.compatibility`` is set to ``BACKWARD`` or
``FULL``, the Hive table schema for a topic will be equivalent to the latest schema in the HDFS files
under that topic that can query the whole data of that topic. If the ``schema.compatibility`` is
set to ``FORWARD``, the Hive table schema of a topic is equivalent to the oldest schema of the HFDS
files under that topic that can query the whole data of that topic.
