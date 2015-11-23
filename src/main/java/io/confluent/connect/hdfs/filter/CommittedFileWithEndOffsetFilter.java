package io.confluent.connect.hdfs.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class CommittedFileWithEndOffsetFilter implements PathFilter {
  private CommittedFileFilter filter;
  private long endOffset;

  public CommittedFileWithEndOffsetFilter(CommittedFileFilter filter, long endOffset) {
    this.filter = filter;
    this.endOffset = endOffset;
  }

  @Override
  public boolean accept(Path path) {
    String[] parts = path.getName().split("[\\.|_]");
    return filter.accept(path) && Long.parseLong(parts[3]) == endOffset;
  }
}
