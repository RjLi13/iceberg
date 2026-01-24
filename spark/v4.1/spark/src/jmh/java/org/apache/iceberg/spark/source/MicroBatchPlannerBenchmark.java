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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileGenerationUtil;
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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * MicroBatchPlannerBenchmark
 *
 * <p>Measures driver-side latency of latestOffset() as table history grows.
 *
 * <p>HYPOTHESIS:
 *
 * <ul>
 *   <li>Sync Planner: Latency scales O(N) with snapshot count (driver performs catalog I/O).
 *   <li>Async Planner: Latency remains O(1) (driver polls pre-filled queue).
 * </ul>
 *
 * <p>METHODOLOGY:
 *
 * <ul>
 *   <li>Pre-populate table with N snapshots (10-15 files per snapshot).
 *   <li>Set ReadLimit.maxFiles(N * 12) to force scanning all history.
 *   <li>Async: 5s warmup allows background thread to pre-fill queue.
 *   <li>Sync: Driver thread scans N snapshots on every call.
 *   <li>Measure: latestOffset() latency.
 *   <li>Production scale: 500-1000 snapshots test real-world streaming tables.
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class MicroBatchPlannerBenchmark {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  @Param({"10", "50", "200", "500", "1000"})
  private int numSnapshots;

  @Param({"true", "false"})
  private boolean asyncEnabled;

  private SparkSession spark;
  private Table table;
  private SparkMicroBatchPlanner planner;
  private StreamingOffset startOffset;
  private ReadLimit readLimit;
  private HadoopCatalog catalog;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    setupInfrastructure();
    populateHistory(numSnapshots);
    initializePlanner();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (planner instanceof AutoCloseable) {
      try {
        ((AutoCloseable) planner).close();
      } catch (Exception e) {
        // ignore
      }
    }
    catalog.dropTable(TableIdentifier.of("default", "honest_bench"), true);
    spark.stop();
  }

  @Benchmark
  public Object measurePlanningLatency(Blackhole blackhole) {
    return planner.latestOffset(startOffset, readLimit);
  }

  private void initializePlanner() throws InterruptedException {
    ImmutableMap<String, String> options =
        ImmutableMap.of(
            SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED,
            String.valueOf(asyncEnabled),
            SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH,
            "100");
    SparkReadConf conf = new SparkReadConf(spark, table, options);

    Snapshot firstSnapshot = table.snapshots().iterator().next();
    this.startOffset = new StreamingOffset(firstSnapshot.snapshotId(), 0, false);
    // Scale limit with snapshot count to force scanning all history
    // Multiply by 12 because we add ~12 files per snapshot
    this.readLimit = ReadLimit.maxFiles(numSnapshots * 12);

    if (asyncEnabled) {
      planner = new AsyncSparkMicroBatchPlanner(table, conf, startOffset, null, null);
      System.out.println("Warming up Async Queue...");
      Thread.sleep(5000);
    } else {
      planner = new SyncSparkMicroBatchPlanner(table, conf, null);
    }
  }

  private void setupInfrastructure() {
    Configuration conf = new Configuration();
    String warehousePath = conf.get("hadoop.tmp.dir") + "/warehouse-" + UUID.randomUUID();
    this.catalog = new HadoopCatalog(conf, warehousePath);
    this.spark = SparkSession.builder().master("local[*]").getOrCreate();
    this.table = catalog.createTable(TableIdentifier.of("default", "honest_bench"), SCHEMA);
  }

  private void populateHistory(int count) {
    StructLike partition = TestHelpers.Row.of();
    int filesPerSnapshot = 10 + (int) (Math.random() * 6);
    for (int i = 0; i < count; i++) {
      RowDelta delta = table.newRowDelta();
      for (int j = 0; j < filesPerSnapshot; j++) {
        DataFile dataFile = FileGenerationUtil.generateDataFile(table, partition);
        delta.addRows(dataFile);
      }
      delta.commit();
    }
  }
}
