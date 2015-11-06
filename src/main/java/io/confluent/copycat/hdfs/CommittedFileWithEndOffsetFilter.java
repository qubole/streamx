package io.confluent.copycat.hdfs;

import org.apache.hadoop.fs.Path;

public class CommittedFileWithEndOffsetFilter extends CommittedFileFilter {
  private long endOffset;

  public CommittedFileWithEndOffsetFilter(long endOffset) {
    this.endOffset = endOffset;
  }

  @Override
  public boolean accept(Path path) {
    String[] parts = path.getName().split("_");
    return super.accept(path) && Long.parseLong(parts[1]) == endOffset;
  }
}
