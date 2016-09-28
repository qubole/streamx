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

package com.qubole.streamx.s3;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class S3SinkConnectorConfig extends HdfsSinkConnectorConfig {
  public static final String S3_URL_CONFIG = "s3.url";
  private static final String S3_URL_DOC =
      "S3 destination where data needs to be written. Format s3://bucket/path/to/dir";
  private static final String S3_URL_DISPLAY = "S3 URL";

  public static final String WAL_CLASS_CONFIG = "wal.class";
  private static final String WAL_CLASS_DOC =
      "WAL implementation to use. Use RDSWAL if you need exactly once guarantee (applies for s3)";
  public static final String WAL_CLASS_DEFAULT = "com.qubole.streamx.s3.wal.DBWAL";
  private static final String WAL_CLASS_DISPLAY = "WAL Class";

  public static final String DB_CONNECTION_URL_CONFIG = "db.connection.url";
  private static final String DB_CONNECTION_URL_DOC =
      "JDBC Connection URL. Required when using DBWAL (which is the default WAL implementation for S3)";
  public static final String DB_CONNECTION_URL_DEFAULT = "";
  private static final String DB_CONNECTION_URL_DISPLAY = "JDBC Connection URL";

  public static final String DB_USER_CONFIG = "db.user";
  private static final String DB_USER_DOC =
      "Name of the User that has access to the database. Required when using DBWAL (which is the default WAL implementation for S3)";
  public static final String DB_USER_DEFAULT = "";
  private static final String DB_USER_DISPLAY = "DB User";

  public static final String DB_PASSWORD_CONFIG = "db.password";
  private static final String DB_PASSWORD_DOC =
      "Password of the user specificed using db.user. Required when using DBWAL (which is the default WAL implementation for S3)";
  public static final String DB_PASSWORD_DEFAULT = "";
  private static final String DB_PASSWORD_DISPLAY = "DB Password";

  public static final String WAL_GROUP = "DBWAL";
  public static final String S3_GROUP = "s3";

  public static final String NAME_CONFIG = "name";
  private static final String NAME_DOC =
      "Name of the connector";
  public static final String NAME_DEFAULT = "";
  private static final String NAME_DISPLAY = "Connector Name";


  static {
    config.define(S3_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, S3_URL_DOC, S3_GROUP, 1, ConfigDef.Width.MEDIUM, S3_URL_DISPLAY);
    config.define(WAL_CLASS_CONFIG, ConfigDef.Type.STRING, WAL_CLASS_DEFAULT, ConfigDef.Importance.LOW, WAL_CLASS_DOC, WAL_GROUP, 1, ConfigDef.Width.MEDIUM, WAL_CLASS_DISPLAY);
    config.define(DB_CONNECTION_URL_CONFIG, ConfigDef.Type.STRING, DB_CONNECTION_URL_DEFAULT, ConfigDef.Importance.LOW, DB_CONNECTION_URL_DOC, WAL_GROUP, 1, ConfigDef.Width.MEDIUM, DB_CONNECTION_URL_DISPLAY);
    config.define(DB_USER_CONFIG, ConfigDef.Type.STRING, DB_USER_DEFAULT, ConfigDef.Importance.LOW, DB_USER_DOC, WAL_GROUP, 1, ConfigDef.Width.MEDIUM, DB_USER_DISPLAY);
    config.define(DB_PASSWORD_CONFIG, ConfigDef.Type.STRING, DB_PASSWORD_DEFAULT, ConfigDef.Importance.LOW, DB_PASSWORD_DOC, WAL_GROUP, 1, ConfigDef.Width.MEDIUM, DB_PASSWORD_DISPLAY);
    config.define(NAME_CONFIG, ConfigDef.Type.STRING, NAME_DEFAULT, ConfigDef.Importance.HIGH, NAME_DOC, S3_GROUP,1, ConfigDef.Width.MEDIUM, NAME_DISPLAY);
  }


  public S3SinkConnectorConfig(Map<String, String> props) {
        super(props);
    }

}
