package io.confluent.connect.hdfs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FileUtilsTest {
  @Test
  public void testExtractOffset() {
    assertEquals(1001, FileUtils.extractOffset("namespace.topic+1+1000+1001.avro"));
    assertEquals(1001, FileUtils.extractOffset("namespace.topic+1+1000+1001"));
    assertEquals(1001, FileUtils.extractOffset("namespace-topic_stuff.foo+1+1000+1001.avro"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractOffsetInvalid() {
    assertEquals(1001, FileUtils.extractOffset("namespace+topic+1+1000+1001.avro"));
  }
}
