package com.qubole.streamx.s3.wal;

import io.confluent.connect.hdfs.FileUtils;
import org.apache.hadoop.util.StringUtils;
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
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.connect.hdfs.storage.Storage;

public class RDSWal implements  WAL {
    private static final Logger log = LoggerFactory.getLogger(RDSWal.class);
    String database = "wal";
    String tableName;
    Storage storage;
    Connection connection;
    ArrayList<String> tempFiles = new ArrayList<>();
    ArrayList<String> committedFiles = new ArrayList<>();
    boolean beginMarker = false;


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
            acquireLease();

            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            //End Marker - Seen all encoded Partitions, write to DB
            if(beginMarker && committedFile.length()==0) {
                beginMarker = false;
                String tempFilesCommaSeparated = StringUtils.join(",",tempFiles);
                String committedFilesCommaSeparated = StringUtils.join(",",committedFiles);

                String sql = String.format("insert into %s (tempFile,committedFile) values ('%s','%s')", tableName, tempFilesCommaSeparated, committedFilesCommaSeparated);
                log.info("committing " + sql);
                statement.executeUpdate(sql);
                connection.commit();
                return;
            }
            //Begin Marker
            else if(committedFile.length()==0) {
                beginMarker = true;
                tempFiles.clear();
                committedFiles.clear();
                return;
            }
            tempFiles.add(tempFile);
            committedFiles.add(committedFile);
        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }

    @Override
    public void apply() throws ConnectException {
        try {
            acquireLease();

            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            String sql = String.format("select * from %s order by id desc limit 1", tableName);
            log.info("Reading wal " + sql);
            ResultSet rs=statement.executeQuery(sql);
            while(rs.next()) {
                String tempFiles = rs.getString(2);
                String committedFiles = rs.getString(3);
                String tempFile[]=tempFiles.split(",");
                String committedFile[]=committedFiles.split(",");
                //TODO : check if all tempFiles are there.
                try {
                    for(int k=0;k<tempFile.length;k++) {
                        storage.commit(tempFile[k], committedFile[k]);
                    }
                } catch (IOException e){
                    e.printStackTrace();
                    throw new ConnectException(e);
                }
            }
        } catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }

    }


    @Override
    public void truncate() throws ConnectException {
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            String sql = String.format("select * from %s order by id desc limit 2", tableName);
            ResultSet rs = statement.executeQuery(sql);
            int rows = 0;
            while(rs.next()){
                rows++;
            }
            if(rows < 2)
                return;
            rs.absolute(2);
            String id = rs.getString(1);
            sql = String.format("delete from %s where id < %s", tableName, id);
            log.info("truncating table " + sql);
            statement.executeUpdate(sql);
            connection.commit();

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
        return tableName;
    }

    @Override
    public long readOffsetFromWAL() {
        ResultSet rs = fetch();
        String committedFiles[];
        long offset = 0;
        try {
            rs.absolute(1);
            committedFiles = rs.getString(3).split(",");
            boolean lastCommittedRecordExists = checkFileExists(committedFiles);
            if (lastCommittedRecordExists) {
                offset = FileUtils.extractOffset(committedFiles[0]) + 1;
            } else {
                rs.absolute(2);
                committedFiles = rs.getString(3).split(",");
                lastCommittedRecordExists = checkFileExists(committedFiles);
                if (!lastCommittedRecordExists) {
                    throw new ConnectException("Unable to recover");
                }
                offset = FileUtils.extractOffset(committedFiles[0]) + 1;
            }

        } catch (SQLException e) {
            log.error("Exception while reading offset from WAL " + e.toString());
        }
        return offset;
    }

    private boolean checkFileExists(String files[]) {
        boolean fileExists = true;
        for(String file: files) {
            try {
                if (!storage.exists(file)) {
                    fileExists = false;
                }
            }catch (IOException e) {
                fileExists = false;
                break;
            }
        }
        return fileExists;
    }

    private ResultSet fetch() throws ConnectException {
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            String sql = String.format("select * from %s order by id desc limit 2", tableName);
            ResultSet rs = statement.executeQuery(sql);
            return rs;
        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }
}
