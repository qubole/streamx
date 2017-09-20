package com.qubole.streamx.format.csv;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes CSV data from a {@link org.apache.kafka.connect.storage.StringConverter} and converts it to Parquet format
 * prior to S3 upload.
 */
public class CsvParquetFormat implements Format {

    private static final Logger log = LoggerFactory.getLogger(CsvParquetFormat.class);

    @Override
    public RecordWriterProvider getRecordWriterProvider() {
        return new CsvParquetRecordWriterProvider();
    }

    @Override
    public SchemaFileReader getSchemaFileReader(AvroData avroData) {
        return null;
    }

    @Override
    public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
        return null;
    }
}
