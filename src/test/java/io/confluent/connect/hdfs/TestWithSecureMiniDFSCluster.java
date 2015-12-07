/**
 * Copyright 2015 Confluent Inc.
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

package io.confluent.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.junit.Assert.assertTrue;

public class TestWithSecureMiniDFSCluster extends HdfsSinkConnectorTestBase {

  private static File baseDir;
  private static String hdfsPrincipal;
  private static MiniKdc kdc;
  private static String keytab;
  private static String spnegoPrincipal;
  private static String connectorPrincipal;
  private static String connectorKeytab;

  private MiniDFSCluster cluster;
  private FileSystem fs;

  @BeforeClass
  public static void initKdc() throws Exception {
    baseDir = new File(System.getProperty("test.build.dir", "target/test-dir"));
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());
    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    File keytabFile = new File(baseDir, "hdfs" + ".keytab");
    keytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, "hdfs" + "/localhost", "HTTP/localhost");
    hdfsPrincipal = "hdfs" + "/localhost@" + kdc.getRealm();
    spnegoPrincipal = "HTTP/localhost@" + kdc.getRealm();

    keytabFile = new File(baseDir, "connect-hdfs" + ".keytab");
    connectorKeytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, "connect-hdfs/localhost");
    connectorPrincipal = "connect-hdfs/localhost@" + kdc.getRealm();
  }

  @AfterClass
  public static void shutdownKdc() {
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(baseDir);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf = createSecureConfig("authentication");
    cluster = createDFSCluster(conf);
    cluster.waitActive();
    url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
    fs = cluster.getFileSystem();
    Map<String, String> props = createProps();
    connectorConfig = new HdfsSinkConnectorConfig(props);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown(true);
    }
    super.tearDown();
  }

  private MiniDFSCluster createDFSCluster(Configuration conf) throws IOException {
    MiniDFSCluster cluster;
    String[] hosts = {"localhost", "localhost", "localhost"};
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.hosts(hosts).nameNodePort(9001).numDataNodes(3);
    cluster = builder.build();
    cluster.waitActive();
    return cluster;
  }

  private Configuration createSecureConfig(String dataTransferProtection) throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, dataTransferProtection);
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);
    conf.set(DFS_ENCRYPT_DATA_TRANSFER_KEY,
             "true");//https://issues.apache.org/jira/browse/HDFS-7431
    String keystoresDir = baseDir.getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(this.getClass());
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    return conf;
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.HDFS_URL_CONFIG, url);
    props.put(HdfsSinkConnectorConfig.HDFS_AUTHENTICATION_KERBEROS_CONFIG, "true");
    // if we use the connect principal to authenticate with secure Hadoop, the following
    // error shows up: Auth failed for 127.0.0.1:63101:null (GSS initiate failed).
    // As a workaround, we temporarily use namenode principal to authenticate the connector
    // with Hadoop.  The error is probably due to the issue of FQDN in Kerberos.
    // As we have tested the connector on secure multi-node cluster, we will figure out
    // the root cause later.
    props.put(HdfsSinkConnectorConfig.CONNECT_HDFS_PRINCIPAL_CONFIG, hdfsPrincipal);
    props.put(HdfsSinkConnectorConfig.CONNECT_HDFS_KEYTAB_CONFIG, keytab);
    props.put(HdfsSinkConnectorConfig.HDFS_NAMENODE_PRINCIPAL_CONFIG, hdfsPrincipal);
    return props;
  }
}
