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
    RunConfig config = config();
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

  private static RunConfig config() {
    return new RunConfig(
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
  }

  @Test
  void malformedFirstRecordDoesNotAnchorPacedReplayToZero() throws Exception {
    Path input = dir.resolve("quarantine.csv");
    Files.writeString(
        input,
        CsvEventReader.HEADER + "\ninvalid\n2,1700000000000000000,TEST_A,QUOTE,99,20,101,20,,\n");
    var c = config();
    var diagnostic =
        new RunConfig(
            c.initialCash(),
            c.feePerShare(),
            c.orderQuantity(),
            c.maxOrderQuantity(),
            c.maxPosition(),
            c.quoteFreshnessNs(),
            0,
            c.timeoutNs(),
            c.buySlippageTicks(),
            true,
            true,
            c.symbols());
    var manifest =
        new DatasetManifest(
            1,
            "fixture",
            42,
            2,
            List.of("TEST_A"),
            10000,
            DatasetManifest.hash(input),
            "synthetic");
    var e = new TradingEngine(diagnostic, new EngineState(c.initialCash()));
    var repository =
        new RunRepository() {
          public void commitBatch(BatchResult b, EngineState state) {}

          public void close() {}
        };
    assertTimeoutPreemptively(
        java.time.Duration.ofSeconds(2),
        () ->
            new ReplayRunner(new Metrics(), new AtomicBoolean())
                .run(
                    input,
                    manifest,
                    c.symbols().keySet(),
                    e,
                    repository,
                    new ReplayRunner.Options(100, 1, 1),
                    ignored -> {}));
    assertTrue(e.state().finalized);
    assertEquals(3, e.state().nextIndex);
    assertEquals(1, e.state().quarantined);
  }

  @Test
  void publicationWaitsForCommitAndIsAbsentOnFailure() throws Exception {
    Path input = dir.resolve("publication.csv");
    DatasetManifest.generate(input, 42, 10);
    var c = config();
    var entered = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    List<String> publications = Collections.synchronizedList(new ArrayList<>());
    var repository =
        new RunRepository() {
          public void commitBatch(BatchResult b, EngineState state) throws Exception {
            entered.countDown();
            release.await();
            throw new java.io.IOException("commit failed");
          }

          public void close() {}
        };
    var task =
        new java.util.concurrent.FutureTask<Void>(
            () -> {
              new ReplayRunner(new Metrics(), new AtomicBoolean())
                  .run(
                      input,
                      DatasetManifest.verify(input, c.symbols().keySet()),
                      c.symbols().keySet(),
                      new TradingEngine(c, new EngineState(c.initialCash())),
                      repository,
                      new ReplayRunner.Options(3, 1, 0),
                      publications::add);
              return null;
            });
    Thread thread = Thread.ofPlatform().start(task);
    try {
      assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
      assertTrue(publications.isEmpty());
      release.countDown();
      var failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> task.get(5, java.util.concurrent.TimeUnit.SECONDS));
      assertInstanceOf(java.io.IOException.class, failure.getCause());
      assertTrue(publications.isEmpty());
    } finally {
      release.countDown();
      thread.interrupt();
      thread.join(5000);
    }
  }
}
