package dev.tickforge.engine;

import static org.junit.jupiter.api.Assertions.*;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.io.*;
import dev.tickforge.ops.Metrics;
import dev.tickforge.persistence.RunRepository;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class ReplayRunnerTest {
  @TempDir Path dir;

  @Test
  void boundedQueueBackpressureAndFailureCancellation() throws Exception {
    Path input = dir.resolve("input.csv");
    DatasetManifest.generate(input, 42, 1000);
    RunConfig config =
        new RunConfig(
            1000000000,
            1,
            10,
            100,
            100,
            1000000000,
            0,
            5000000000L,
            200,
            false,
            false,
            Map.of(
                "TEST_A",
                new RunConfig.Threshold(1000000, 1002000),
                "TEST_B",
                new RunConfig.Threshold(1000000, 1002000)));
    Metrics metrics = new Metrics();
    AtomicBoolean stop = new AtomicBoolean();
    AtomicInteger commits = new AtomicInteger();
    RunRepository slow =
        new RunRepository() {
          public void commitBatch(BatchResult b, EngineState s) throws Exception {
            Thread.sleep(20);
            if (commits.incrementAndGet() == 3) throw new java.io.IOException("database failed");
          }

          public void close() {}
        };
    assertTimeoutPreemptively(
        java.time.Duration.ofSeconds(5),
        () ->
            assertThrows(
                java.io.IOException.class,
                () ->
                    new ReplayRunner(metrics, stop)
                        .run(
                            input,
                            DatasetManifest.verify(input, config.symbols().keySet()),
                            config.symbols().keySet(),
                            new TradingEngine(config, new EngineState(config.initialCash())),
                            slow,
                            new ReplayRunner.Options(1, 1, 0),
                            s -> {})));
    assertTrue(stop.get());
    assertTrue(metrics.blockedNs.get() > 0);
    assertEquals(2, metrics.committed.get());
    assertEquals(1, metrics.commitFailures.get());
  }
}
