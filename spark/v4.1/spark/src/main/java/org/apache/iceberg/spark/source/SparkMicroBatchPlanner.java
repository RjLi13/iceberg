/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;

/**
 * Interface for planning micro-batches in Iceberg streaming reads.
 *
 * <p>Implementations can provide different strategies for planning files, such as synchronous
 * manifest scanning or asynchronous background processing with queuing.
 */
interface SparkMicroBatchPlanner {

  /**
   * Plans the files to be read between the start and end offsets.
   *
   * @param start the starting offset (exclusive)
   * @param end the ending offset (inclusive)
   * @return list of file scan tasks to be processed
   */
  List<FileScanTask> planFiles(StreamingOffset start, StreamingOffset end);

  /**
   * Determines the latest offset that can be processed given the start offset and read limit.
   *
   * @param start the starting offset
   * @param limit the read limit (max files or max rows)
   * @return the latest offset that respects the limit, or null if no new data
   */
  StreamingOffset latestOffset(StreamingOffset start, ReadLimit limit);

  /**
   * Performs cleanup when the stream is stopped. Implementations should release any resources such
   * as executors, queues, or background threads.
   */
  void stop();
}
