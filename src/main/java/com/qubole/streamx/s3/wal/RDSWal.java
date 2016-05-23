package com.qubole.streamx.s3.wal;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import io.confluent.connect.hdfs.wal.WAL;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DatabaseMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.connect.hdfs.storage.Storage;

public class RDSWal implements  WAL {
    private static final Logger log = LoggerFactory.getLogger(RDSWal.class);
    String database = "wal";
    String tableName;
    Storage storage;
    Connection connection;
    public RDSWal(String logsDir, TopicPartition topicPartition, Storage storage) {
        this.storage = storage;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        tableName = topicPartition.topic()+topicPartition.partition();
        try {

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/kafka","root","sss");
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            DatabaseMetaData dbm = connection.getMetaData();

            ResultSet tables = dbm.getTables(null, null, tableName, null);
            if (tables.next()) {
                // No op
            }
            else {
                String sql=String.format("create table %s (id INT AUTO_INCREMENT, tempFile VARCHAR(500), committedFile VARCHAR(500), primary key (id))", tableName);
                log.info("Creating table "+ sql);
                statement.executeUpdate(sql);
                connection.commit();
            }

        }catch (SQLException e) {
            log.error(e.toString());
            throw new ConnectException(e);

        }
    }
    @Override
    public void acquireLease() throws ConnectException {
        //Implement a lease that keep's renewing as long as thread is alive
    }

    @Override
    public void append(String tempFile, String committedFile) throws ConnectException {
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            if(committedFile.length()!=0) {
                String sql = String.format("insert into %s (tempFile,committedFile) values ('%s','%s')", tableName, tempFile, committedFile);
                log.info("committing " + sql);
                statement.executeUpdate(sql);
                connection.commit();
            }
        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }

    @Override
        public void apply() throws ConnectException {
            try {
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(30);  // set timeout to 30 sec.

                String sql = String.format("select * from %s order by id desc limit 1", tableName);
                log.info("Reading wal " + sql);
                ResultSet rs=statement.executeQuery(sql);
                while(rs.next()) {
                    String tempFile = rs.getString(2);
                    String committedFile = rs.getString(3);
                    try {
                        storage.commit(tempFile, committedFile);
                    } catch (IOException e){
                        e.printStackTrace();
                        throw new ConnectException(e);
                    }
                }

            }catch (SQLException e){
                log.error(e.toString());
                throw new ConnectException(e);
            }

        }


    @Override
    public void truncate() throws ConnectException {
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            String sql = String.format("truncate table %s", tableName);
            log.info("truncating table " + sql);
            statement.executeUpdate(sql);

        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }

    @Override
    public void close() throws ConnectException {

    }

    @Override
    public String getLogFile() {
        return null;
    }
}
