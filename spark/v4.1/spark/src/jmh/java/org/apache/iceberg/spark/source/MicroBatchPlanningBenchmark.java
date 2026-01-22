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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileGenerationUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the latestOffset() performance for sync vs async micro-batch planning.
 *
 * <p>Tests manifest iteration performance with many small snapshots - the key bottleneck async
 * planning addresses.
 *
 * <p>To run this benchmark for spark-4.1:
 *
 * <pre>{@code
 * ./gradlew -DsparkVersions=4.1 :iceberg-spark:iceberg-spark-4.1_2.13:jmh \
 *     -PjmhIncludeRegex=MicroBatchPlanningBenchmark \
 *     -PjmhOutputPath=benchmark/micro-batch-planning-benchmark.txt
 * }</pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
public class MicroBatchPlanningBenchmark {

  private static final int FILES_PER_SNAPSHOT = 100;
  private static final int MAX_FILES_PER_MICRO_BATCH = 50;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private final Configuration hadoopConf = new Configuration();

  @Param({"true", "false"})
  private boolean asyncEnabled;

  @Param({"100", "500", "1000"})
  private int numSnapshots;

  private SparkSession spark;
  private Table table;
  private SparkMicroBatchPlanner planner;
  private HadoopCatalog catalog;
  private StreamingOffset initialOffset;

  @Setup
  public void setup() throws IOException {
    setupSpark();
    setupCatalog();
    createTable();
    populateSnapshots();
    initializePlanner();
  }

  @TearDown
  public void tearDown() {
    if (planner instanceof AutoCloseable) {
      try {
        ((AutoCloseable) planner).close();
      } catch (Exception e) {
        // Ignore
      }
    }
    dropTable();
    spark.stop();
  }

  @Benchmark
  @Threads(1)
  public void latestOffset(Blackhole blackhole) {
    // Simulate multiple micro-batch cycles to measure consistent low latency for latestOffset
    // This is the key metric: async planner should return near-instantly from pre-filled queue
    StreamingOffset start = initialOffset;
    ReadLimit limit = new CustomReadAllAvailable();
    for (int i = 0; i < 20; i++) {
      StreamingOffset offset = planner.latestOffset(start, limit);
      blackhole.consume(offset);
      if (offset != null && !offset.equals(StreamingOffset.START_OFFSET)) {
        start = offset;
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void fullPlanningCycle(Blackhole blackhole) {
    // Measure full planning cycle: latestOffset + planFiles
    StreamingOffset start = initialOffset;
    ReadLimit limit = new CustomReadAllAvailable();
    for (int i = 0; i < 20; i++) {
      StreamingOffset end = planner.latestOffset(start, limit);
      if (end != null
          && !end.equals(StreamingOffset.START_OFFSET)
          && end.snapshotId() > start.snapshotId()) {
        List<FileScanTask> tasks = planner.planFiles(start, end);
        blackhole.consume(tasks);
        start = end;
      }
    }
  }

  // Custom ReadLimit implementation for benchmarking
  private static class CustomReadAllAvailable implements ReadLimit {}

  private void setupSpark() {
    this.spark =
        SparkSession.builder()
            .config("spark.ui.enabled", false)
            .master("local[*]")
            .getOrCreate();
  }

  private void setupCatalog() {
    String warehouseLocation = hadoopConf.get("hadoop.tmp.dir") + "/warehouse-" + UUID.randomUUID();
    this.catalog = new HadoopCatalog(hadoopConf, warehouseLocation);
  }

  private void createTable() {
    TableIdentifier tableId = TableIdentifier.of("default", "micro_batch_benchmark");
    this.table = catalog.createTable(tableId, SCHEMA);
  }

  private void dropTable() {
    TableIdentifier tableId = TableIdentifier.of("default", "micro_batch_benchmark");
    if (catalog.tableExists(tableId)) {
      catalog.dropTable(tableId, true);
    }
  }

  private void populateSnapshots() {
    // Create many snapshots with small number of files each
    // This simulates the scenario where async planning helps most:
    // many small incremental commits requiring frequent manifest iteration
    StructLike partition = TestHelpers.Row.of();

    for (int snapshotNum = 0; snapshotNum < numSnapshots; snapshotNum++) {
      RowDelta rowDelta = table.newRowDelta();

      for (int fileNum = 0; fileNum < FILES_PER_SNAPSHOT; fileNum++) {
        DataFile dataFile = FileGenerationUtil.generateDataFile(table, partition);
        rowDelta.addRows(dataFile);
      }

      rowDelta.commit();
    }
  }

  private void initializePlanner() {
    Map<String, String> options =
        ImmutableMap.of(
            SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED,
            String.valueOf(asyncEnabled),
            SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH,
            String.valueOf(MAX_FILES_PER_MICRO_BATCH));

    SparkReadConf readConf = new SparkReadConf(spark, table, options);

    // Get initial offset (start from first snapshot)
    Snapshot firstSnapshot = table.snapshots().iterator().next();
    this.initialOffset = new StreamingOffset(firstSnapshot.snapshotId(), 0, false);

    // Create planner based on async flag
    if (asyncEnabled) {
      this.planner =
          new AsyncSparkMicroBatchPlanner(table, readConf, initialOffset, null, null);
    } else {
      this.planner = new SyncSparkMicroBatchPlanner(table, readConf, null);
    }
  }
}
