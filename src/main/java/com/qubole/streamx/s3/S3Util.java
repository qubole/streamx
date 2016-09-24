package com.qubole.streamx.s3;

/**
 * Static class for S3 utility functions
 */
public class S3Util {
  /**
   * Simply clean topic name for creating database.
   * @param topicName
   * @return
   */
  public static String cleanTopicNameForDBWal(String topicName){
    return topicName.toLowerCase().replaceAll("\\W", "_");
  }
}
