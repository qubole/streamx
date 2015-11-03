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

package io.confluent.copycat.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class CommittedFileFilter implements PathFilter {

  @Override
  public boolean accept(Path path) {
    String filename = path.getName();
    if (filename.equals("log") || filename.equals("log.1")) {
      return false;
    }
    String[] parts = path.getName().split("_");

    return !(parts.length != 2 || parts[1].equals("tmp"));
  }
}
