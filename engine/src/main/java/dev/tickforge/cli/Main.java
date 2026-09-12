package dev.tickforge.cli;

import dev.tickforge.engine.*;
import dev.tickforge.io.*;
import dev.tickforge.ops.*;
import dev.tickforge.persistence.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Main {
  private Main() {}

  public static void main(String[] args) {
    try {
      execute(args);
    } catch (Exception e) {
      System.err.println(
          Json.encode(
              Map.of(
                  "level",
                  "ERROR",
                  "reason",
                  e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage())));
      System.exit(1);
    }
  }

  static void execute(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("TickForge: generate | replay | resume | report (see README.md)");
      return;
    }
    String command = args[0];
    Map<String, String> options = new HashMap<>();
    for (int i = 1; i < args.length; i += 2) {
      if (i + 1 >= args.length
          || !args[i].startsWith("--")
          || options.put(args[i].substring(2), args[i + 1]) != null)
        throw new IllegalArgumentException("expected unique --option value pairs");
    }
    Set<String> allowed =
        switch (command) {
          case "generate" -> Set.of("output", "seed", "events");
          case "report" -> Set.of("run-id", "format");
          case "replay", "resume" ->
              Set.of("run-id", "input", "config", "batch-size", "queue-capacity", "speed", "port");
          default -> throw new IllegalArgumentException("unknown command: " + command);
        };
    if (!allowed.containsAll(options.keySet()))
      throw new IllegalArgumentException("unknown option");
    if (command.equals("generate")) {
      DatasetManifest.generate(
          Path.of(required(options, "output")),
          Long.parseLong(options.getOrDefault("seed", "42")),
          Long.parseLong(options.getOrDefault("events", "100000")));
      return;
    }
    String run = required(options, "run-id");
    if (command.equals("report")) {
      if (!options.getOrDefault("format", "json").equals("json"))
        throw new IllegalArgumentException("only JSON report format is supported");
      try (var repository = new JdbcRunRepository(connect(), run)) {
        repository.writeCanonicalReport(System.out);
        System.out.println();
        if (System.out.checkError()) throw new java.io.IOException("report output failed");
      }
      return;
    }
    var metrics = new Metrics();
    AtomicBoolean stop = new AtomicBoolean();
    CountDownLatch finished = new CountDownLatch(1);
    Thread hook =
        new Thread(
            () -> {
              stop.set(true);
              try {
                finished.await(25, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            },
            "tickforge-shutdown");
    Runtime.getRuntime().addShutdownHook(hook);
    try (var status =
            new StatusServer(Integer.parseInt(options.getOrDefault("port", "0")), metrics);
        var repo = new JdbcRunRepository(connect(), run)) {
      boolean resume = command.equals("resume");
      var metadata = resume ? repo.metadata() : null;
      Path input =
          Path.of(
              options.getOrDefault(
                  "input", resume ? metadata.inputPath() : required(options, "input")));
      RunConfig config =
          options.containsKey("config")
              ? RunConfig.load(Path.of(options.get("config")))
              : resume
                  ? Json.decode(metadata.configJson(), RunConfig.class)
                  : RunConfig.load(Path.of("config/demo.yaml"));
      var manifest = DatasetManifest.verify(input, config.symbols().keySet());
      var replayOptions =
          new ReplayRunner.Options(
              Integer.parseInt(options.getOrDefault("batch-size", "100")),
              Integer.parseInt(options.getOrDefault("queue-capacity", "8192")),
              Double.parseDouble(options.getOrDefault("speed", "0")));
      var engine = new TradingEngine(config, repo.initialize(input, manifest, config, resume));
      if (resume) metrics.recoveries.incrementAndGet();
      metrics.committedState(engine.state());
      status.publish(Json.encode(engine.state()));
      status.ready.set(true);
      System.err.println(
          Json.encode(
              Map.of(
                  "level",
                  "INFO",
                  "runId",
                  run,
                  "event",
                  "ready",
                  "port",
                  status.port(),
                  "nextIndex",
                  engine.state().nextIndex,
                  "buildId",
                  JdbcRunRepository.BUILD_ID)));
      long start = System.nanoTime();
      new ReplayRunner(metrics, stop)
          .run(
              input,
              manifest,
              config.symbols().keySet(),
              engine,
              repo,
              replayOptions,
              status::publish);
      status.ready.set(false);
      System.out.println(
          Json.encode(
              Map.of(
                  "runId",
                  run,
                  "finalized",
                  engine.state().finalized,
                  "nextIndex",
                  engine.state().nextIndex,
                  "cashTicks",
                  engine.state().cash,
                  "positions",
                  engine.state().positions,
                  "feesTicks",
                  engine.state().fees,
                  "fillCount",
                  engine.state().fillCount,
                  "rejections",
                  engine.state().rejections,
                  "elapsedNs",
                  System.nanoTime() - start,
                  "metrics",
                  metrics.snapshot())));
    } finally {
      finished.countDown();
      try {
        Runtime.getRuntime().removeShutdownHook(hook);
      } catch (IllegalStateException ignored) {
      }
    }
  }

  private static String required(Map<String, String> options, String name) {
    String value = options.get(name);
    if (value == null || value.isBlank()) throw new IllegalArgumentException("missing --" + name);
    return value;
  }

  public static Connection connect() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", System.getenv().getOrDefault("TICKFORGE_DB_USER", "tickforge"));
    props.setProperty("password", System.getenv().getOrDefault("TICKFORGE_DB_PASSWORD", ""));
    props.setProperty("connectTimeout", "10");
    props.setProperty("socketTimeout", "20");
    return DriverManager.getConnection(
        System.getenv()
            .getOrDefault("TICKFORGE_DB_URL", "jdbc:postgresql://localhost:55432/tickforge"),
        props);
  }
}
