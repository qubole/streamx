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

package io.confluent.connect.hdfs.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CommittedFileFilter implements PathFilter {
  private static Pattern pattern = Pattern.compile("[a-zA-Z0-9\\._\\-]+\\+\\d+\\+\\d+\\+\\d+(.\\w+)?");

  @Override
  public boolean accept(Path path) {
    String filename = path.getName();
    Matcher m = pattern.matcher(filename);
    return m.matches();
  }
}
