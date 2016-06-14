package com.qubole.streamx.s3;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class S3SinkConnectorConfig extends HdfsSinkConnectorConfig {

    public static final String WAL_CLASS_CONFIG = "wal.class";
    private static final String WAL_CLASS_DOC =
            "WAL implementation to use. Use RDSWAL if you need exactly once guarantee (applies for s3)";
    public static final String WAL_CLASS_DEFAULT = "com.qubole.streamx.s3.wal.DBWAL";
    private static final String WAL_CLASS_DISPLAY = "WAL Class";

    static {
        config.define(WAL_CLASS_CONFIG, ConfigDef.Type.STRING, WAL_CLASS_DEFAULT, ConfigDef.Importance.LOW, WAL_CLASS_DOC, INTERNAL_GROUP, 1, ConfigDef.Width.MEDIUM, WAL_CLASS_DISPLAY);
    }


    public S3SinkConnectorConfig(Map<String, String> props) {
        super(props);
    }

}
