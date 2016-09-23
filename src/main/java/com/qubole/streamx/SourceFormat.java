package com.qubole.streamx;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;

public class SourceFormat implements Format{
    @java.lang.Override
    public RecordWriterProvider getRecordWriterProvider() {
        return new SourceRecordWriterProvider();
    }

    @java.lang.Override
    public SchemaFileReader getSchemaFileReader(AvroData avroData) {
        return null;
    }

    @java.lang.Override
    public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
        return null;
    }
}
