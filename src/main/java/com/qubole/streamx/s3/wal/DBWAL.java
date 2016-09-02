/**
 * Copyright 2015 Qubole Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package com.qubole.streamx.s3.wal;

import com.qubole.streamx.s3.S3SinkConnectorConfig;
import com.qubole.streamx.s3.S3Util;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.wal.WAL;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class DBWAL implements  WAL {
    private static final Logger log = LoggerFactory.getLogger(DBWAL.class);
    String tableName;
    Storage storage;
    Connection connection;
    ArrayList<String> tempFiles = new ArrayList<>();
    ArrayList<String> committedFiles = new ArrayList<>();
    int partitionId = -1;
    int id = ThreadLocalRandom.current().nextInt(1, 100000 + 1);
    HdfsSinkConnectorConfig config;
    String lease_table = "streamx_lease";
    AbstractDBWALAccessor dbwalAccessor;

    public DBWAL(String logsDir, TopicPartition topicPartition, Storage storage, HdfsSinkConnectorConfig config) {
        this.storage = storage;
        this.config = config;
        partitionId = topicPartition.partition();


        try {
            String name = config.getString(S3SinkConnectorConfig.NAME_CONFIG);
            tableName = name + "_" + S3Util.cleanTopicNameForDBWal(topicPartition.topic()) + "_" + partitionId;

            String connectionURL = config.getString(S3SinkConnectorConfig.DB_CONNECTION_URL_CONFIG);
            String user = config.getString(S3SinkConnectorConfig.DB_USER_CONFIG);
            String password = config.getString(S3SinkConnectorConfig.DB_PASSWORD_CONFIG);
            if(connectionURL.length()==0 || user.length()==0 || password.length()==0)
                throw new ConnectException("db.connection.url,db.user,db.password - all three properties must be specified");
            log.info("jdbc wal connecting to " + connectionURL);
            dbwalAccessor = AbstractDBWALAccessor.getInstance(connectionURL, user, password, tableName);
            connection = DriverManager.getConnection(connectionURL, user, password);
            dbwalAccessor.createWalTable(name, topicPartition);
            dbwalAccessor.createLeaseTable();

        }catch (SQLException e) {
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }

    @Override
    public void acquireLease() throws ConnectException {

        long sleepIntervalMs = 1000L;
        long MAX_SLEEP_INTERVAL_MS = 16000L;
        while (sleepIntervalMs < MAX_SLEEP_INTERVAL_MS) {
            try {
                ResultSet rs = dbwalAccessor.getLeaseResultSetLockedRow();
                if(!rs.next()) {
                    dbwalAccessor.insertLeaseTableRow(id);
                    return;
                }

                if(canAcquireLock(rs)) {
                    dbwalAccessor.updateLeaseTableRow(id);
                    return;
                }
                connection.commit();
            } catch (SQLException e) {
                log.error(e.toString());
                throw new ConnectException(e);
            }
            try {
                Thread.sleep(sleepIntervalMs);
            } catch (InterruptedException ie) {
                throw new ConnectException(ie);
            }
            sleepIntervalMs = sleepIntervalMs * 2;
            if (sleepIntervalMs >= MAX_SLEEP_INTERVAL_MS) {
                throw new ConnectException("Cannot acquire lease after timeout, will retry.");
            }
        }
    }
    private boolean canAcquireLock(ResultSet rs) {
        try {
            boolean exists = rs.next();
            if(!exists)
                return true;
            java.sql.Timestamp now = rs.getTimestamp("currentTS");
            java.sql.Timestamp ts = rs.getTimestamp("ts");

            if(now.getTime() - ts.getTime() >= 60*1000) {
                log.warn("last update is more than a minute" + now + " " + ts);
                return false;
            }
            else {
                log.warn("last update within a minute"+ now + " " + ts);
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void append(String tempFile, String committedFile) throws ConnectException {
        try {
            if(WAL.beginMarker.equals(tempFile)) {
                tempFiles.clear();
                committedFiles.clear();
            }
            else if(WAL.endMarker.equals(tempFile)) {
                String tempFilesCommaSeparated = StringUtils.join(",",tempFiles);
                String committedFilesCommaSeparated = StringUtils.join(",",committedFiles);

                acquireLease();
                dbwalAccessor.insertCommitedFile(tempFilesCommaSeparated, committedFilesCommaSeparated);
            }
            else {
                tempFiles.add(tempFile);
                committedFiles.add(committedFile);
            }
        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }

    @Override
    public void apply() throws ConnectException {
        try {
            acquireLease();
            ResultSet rs = dbwalAccessor.getLastResultSetFromWalTable();

            while(rs.next()) {
                String tempFiles = rs.getString("tempFiles");
                String committedFiles = rs.getString("committedFiles");
                String tempFile[]=tempFiles.split(",");
                String committedFile[]=committedFiles.split(",");
                //TODO : check if all tempFiles are there.
                try {
                    for(int k=0;k<tempFile.length;k++) {
                        storage.commit(tempFile[k], committedFile[k]);
                        log.info("Recovering file "+tempFile[k]+" "+committedFile[k]);
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
            ResultSet rs = dbwalAccessor.getLastNResultsetFromWalTable(2);
            int rows = 0;
            while(rs.next()){
                rows++;
            }
            if(rows < 2)
                return;
            rs.absolute(2);
            String id = rs.getString("id");
            dbwalAccessor.truncateTableLessThanId(id);
        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }



    @Override
    public void close() throws ConnectException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new ConnectException("Unable to close connection",e);
        }
    }

    @Override
    public String getLogFile() {
        return tableName;
    }

    @Override
    public long readOffsetFromWAL() {
        ResultSet rs = fetch();
        long offset = -1L;
        try {
            // Check if last committed record in WAL exists in s3
            if(rs.next())
                offset = checkWAlEntryExists(rs);
            // Else pick the previously committed record. This case can happen only
            // if tempFiles are stored in LocalFS
            if(offset == -1 && rs.next())
                offset = checkWAlEntryExists(rs);

            log.info("Offset from WAL " + offset + " for topic partition id " + partitionId);

        } catch (SQLException e) {
            log.error("Exception while reading offset from WAL " + e.toString());
        }
        return offset;
    }

    private long checkWAlEntryExists(ResultSet rs) {
        long offset = -1L;
        String committedFiles[];
        try {
            committedFiles = rs.getString("committedFiles").split(",");
            boolean lastCommittedRecordExists = checkFileExists(committedFiles);
            if (lastCommittedRecordExists)
                offset = FileUtils.extractOffset(committedFiles[0]) + 1;
        } catch (SQLException e) {
            e.printStackTrace();
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
            ResultSet rs = dbwalAccessor.getLastNResultsetFromWalTable(2);
            return rs;
        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }
}
