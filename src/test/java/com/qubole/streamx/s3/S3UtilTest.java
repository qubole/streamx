package com.qubole.streamx.s3;

import org.junit.Test;

public class S3UtilTest {
  @Test
  public void cleanTopicNameForDBWal() throws Exception {
    String name = "S3.Test";
    String expectedName = "s3_test";

    assert S3Util.cleanTopicNameForDBWal(name).equals(expectedName);
  }

}