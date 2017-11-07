package io.confluent.connect.hdfs.partitioner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.PartitionException;
import org.apache.calcite.interpreter.Sink;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

public class MultiFieldParitioner extends FieldPartitioner {

    private static String timeField;
    private static String timeFormat = "yyyymmdd";
    private SimpleDateFormat mDtFormatter;
    private final ObjectMapper mapper = new ObjectMapper();
    private String[] handledDateFormats = {"yyyMMdd", "yyyy-MM-dd'T'HH:mm:ss", "epoch_secs"};


    @Override
    public void configure(Map<String, Object> config) {
        super.configure(config);
        timeField = (String) config.get(HdfsSinkConnectorConfig.PARTITION_TIME_FIELD_NAME_CONFIG);
        timeFormat = (String) config.get(HdfsSinkConnectorConfig.PARTITION_TIME_FIELD_FORMAT_CONFIG);
        mDtFormatter = new SimpleDateFormat(timeFormat);
        mDtFormatter.setTimeZone(TimeZone.getTimeZone((String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG)));
        partitionFields.add(new FieldSchema(fieldName, TypeInfoFactory.stringTypeInfo.toString(), ""));
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        String fieldPartition;
        String timePartition = "dt=" + parseTimeStamp(sinkRecord);
        try{
            fieldPartition = super.encodePartition(sinkRecord);
        } catch (PartitionException e){
            fieldPartition = encodeParitionFromJson(sinkRecord);
        }
        if(fieldPartition == null){
            throw new PartitionException("Error encoding partition.");
        }
        return timePartition + "/" + fieldPartition;
    }

    private String encodeParitionFromJson(SinkRecord sinkRecord){
        try {
            return fieldName + "=" + getValueFromJsonOrAvroRecord(sinkRecord, fieldName);
        } catch (IOException e) {
            throw new PartitionException("Error encoding partition.");
        }
    }

    private String parseTimeStamp(SinkRecord sinkRecord){
        String timeStamp = null;
        try{
            Object timeFieldValue = getValueFromJsonOrAvroRecord(sinkRecord, timeField);
            if(sinkRecord.timestamp() != null){
                Date date = new Date(sinkRecord.timestamp());
                timeStamp = mDtFormatter.format(date);
            } else if(timeFieldValue != null){
                Date date = mDtFormatter.parse(timeFieldValue.toString());
                timeStamp = mDtFormatter.format(date);
            }
        } catch (Exception e){
            throw new PartitionException("Error encoding partition.");
        }
        return timeStamp;
    }

    private Object getValueFromJsonOrAvroRecord(SinkRecord sinkRecord, String fieldName) throws IOException {
        Object value = sinkRecord.value();
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            return struct.get(fieldName);
        } else {
            String jsonString = new String((byte[]) sinkRecord.value());
            JsonNode jsonObj = mapper.readTree(jsonString);
            return jsonObj.get(fieldName).toString();
        }
    }

}
