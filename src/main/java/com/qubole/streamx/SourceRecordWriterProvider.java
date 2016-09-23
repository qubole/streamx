package com.qubole.streamx;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.avro.AvroRecordWriterProvider;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;

public class SourceRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(SourceRecordWriterProvider.class);

    @java.lang.Override
    public String getExtension() {
        return ".json";
    }

    @java.lang.Override
    public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, final String fileName, SinkRecord record, final AvroData avroData) throws IOException {
        //DatumWriter<Object> datumWriter = new GenericDatumWriter<>();
        //final DataFileWriter<Object> writer = new DataFileWriter<>(datumWriter);
        Path path = new Path(fileName);

        //final Schema schema = record.valueSchema();
        final FSDataOutputStream out = path.getFileSystem(conf).create(path);
        //org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
        //writer.create(avroSchema, out);
        //final BufferedWriter br=new BufferedWriter(new OutputStreamWriter(out));

        return new RecordWriter<SinkRecord>() {
            @Override
            public void write(SinkRecord record) throws IOException {
                log.trace("Sink record: {}", record.toString());
                //Object value = avroData.fromConnectData(schema, record.value());
                //writer.append(value);
                byte [] values = (byte[])(record.value());
                out.write(values);
                out.write("\n".getBytes());
            }

            @Override
            public void close() throws IOException {
                out.close();
            }
        };
    }
}
