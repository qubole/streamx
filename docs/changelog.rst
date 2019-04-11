.. _hdfs_connector_changelog:

Changelog
=========

Version 3.3.0
-------------

* `PR-187 <https://github.com/confluentinc/kafka-connect-hdfs/pull/187>`_ - CC-491: Consolidate and simplify unit tests of HDFS connector.
* `PR-205 <https://github.com/confluentinc/kafka-connect-hdfs/pull/205>`_ - Upgrade avro to 1.8.2.

Version 3.2.2
-------------

* `PR-194 <https://github.com/confluentinc/kafka-connect-hdfs/pull/194>`_ - Fix HdfsSinkConnector to extend from SinkConnector instead of Connector.
* `PR-200 <https://github.com/confluentinc/kafka-connect-hdfs/pull/200>`_ - Fix incorrect licensing and webpage info.

Version 3.2.1
-------------
No changes

Version 3.2.0
-------------

* `PR-135 <https://github.com/confluentinc/kafka-connect-hdfs/pull/135>`_ - Fix typos
* `PR-164 <https://github.com/confluentinc/kafka-connect-hdfs/pull/164>`_ - Issue 136 - Support topic with dots in hive.
* `PR-170 <https://github.com/confluentinc/kafka-connect-hdfs/pull/170>`_ - MINOR: Upgrade Hadoop version to 2.7.3 and joda-time to 2.9.7

Version 3.1.1
-------------
No changes

Version 3.1.0
-------------

* `PR-134 <https://github.com/confluentinc/kafka-connect-hdfs/pull/134>`_ - Flush the last partial file when incoming stream is paused.
* `PR-133 <https://github.com/confluentinc/kafka-connect-hdfs/pull/133>`_ - CC-331: Update config options docs
* `PR-126 <https://github.com/confluentinc/kafka-connect-hdfs/pull/126>`_ - Fix TimeBasedPartitioner config validation
* `PR-112 <https://github.com/confluentinc/kafka-connect-hdfs/pull/112>`_ - Lint change to avoid compiler error in Oracle JDK 1.7 using jenv.
* `PR-94 <https://github.com/confluentinc/kafka-connect-hdfs/pull/94>`_ - Fix lint annoyances
* `PR-108 <https://github.com/confluentinc/kafka-connect-hdfs/pull/108>`_ - Revert "support multi partition fields."
* `PR-105 <https://github.com/confluentinc/kafka-connect-hdfs/pull/105>`_ - support multi partition fields.
* `PR-101 <https://github.com/confluentinc/kafka-connect-hdfs/pull/101>`_ - Added link to Confluent documentation for the connector.
* `PR-92 <https://github.com/confluentinc/kafka-connect-hdfs/pull/92>`_ - Start a new WAL file after `truncate` instead of appending to log.1
* `PR-87 <https://github.com/confluentinc/kafka-connect-hdfs/pull/87>`_ - Scheduled rotation implementation
* `PR-90 <https://github.com/confluentinc/kafka-connect-hdfs/pull/90>`_ - Use configured Hadoop configuration object for Parquet writer
* `PR-91 <https://github.com/confluentinc/kafka-connect-hdfs/pull/91>`_ - Upgrade to Hadoop 2.6.1
* `PR-70 <https://github.com/confluentinc/kafka-connect-hdfs/pull/70>`_ - Fix handling of topics with periods
* `PR-68 <https://github.com/confluentinc/kafka-connect-hdfs/pull/68>`_ - prints details of HDFS exceptions
* `PR-67 <https://github.com/confluentinc/kafka-connect-hdfs/pull/67>`_ - clean up hive metastore artifacts from testing
* `PR-64 <https://github.com/confluentinc/kafka-connect-hdfs/pull/64>`_ - cleaned up .gitignore.  Now ignores Eclipse files

Version 3.0.1
-------------

HDFS Connector
~~~~~~~~~~~~~~
* `PR-82 <https://github.com/confluentinc/kafka-connect-hdfs/pull/82>`_ - add version.txt to share/doc

Version 3.0.0
-------------

HDFS Connector
~~~~~~~~~~~~~~
* `PR-62 <https://github.com/confluentinc/kafka-connect-hdfs/pull/62>`_ - Update doc for CP 3.0.
* `PR-60 <https://github.com/confluentinc/kafka-connect-hdfs/pull/60>`_ - Remove HDFS connectivity check.
* `PR-55 <https://github.com/confluentinc/kafka-connect-hdfs/pull/55>`_ - Removing retry logic from HiveMetaStore to fix the metastore connection bloat.
* `PR-50 <https://github.com/confluentinc/kafka-connect-hdfs/pull/50>`_ - Remove close of topic partition writers in DataWriter close.
* `PR-42 <https://github.com/confluentinc/kafka-connect-hdfs/pull/42>`_ - Using new config validation.
* `PR-41 <https://github.com/confluentinc/kafka-connect-hdfs/pull/41>`_ - Bump version to 3.0.0-SNAPSHOT and Kafka dependency to 0.10.0.0-SNAPSHOT.
* `PR-35 <https://github.com/confluentinc/kafka-connect-hdfs/pull/35>`_ - Minor doc typo fix TimeBasedPartitioner.
* `PR-33 <https://github.com/confluentinc/kafka-connect-hdfs/pull/33>`_ - Minor doc fix.
