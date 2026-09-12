package dev.tickforge.persistence;

import static org.junit.jupiter.api.Assertions.*;

import dev.tickforge.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;

@Testcontainers
class ProcessRecoveryIT {
  @Container
  static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:17.6-alpine");

  @TempDir Path dir;
  Path input, config;

  @BeforeEach
  void fixture() throws Exception {
    input = dir.resolve("events.csv");
    DatasetManifest.generate(input, 42, 202);
    config = Path.of("../config/demo.yaml").toAbsolutePath();
  }

  Process start(String run, String command, Map<String, String> env, String... extra)
      throws Exception {
    List<String> args =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar",
                Path.of("target/engine-1.0.0-SNAPSHOT.jar").toAbsolutePath().toString(),
                command,
                "--run-id",
                run));
    if (!command.equals("report")) {
      args.addAll(List.of("--batch-size", "7", "--queue-capacity", "1"));
      if (command.equals("replay"))
        args.addAll(List.of("--input", input.toString(), "--config", config.toString()));
    }
    for (int i = 0; i < extra.length; i += 2) {
      int existing = args.indexOf(extra[i]);
      if (existing >= 0) args.set(existing + 1, extra[i + 1]);
      else args.addAll(List.of(extra[i], extra[i + 1]));
    }
    ProcessBuilder p = new ProcessBuilder(args);
    p.environment().put("TICKFORGE_DB_URL", DB.getJdbcUrl());
    p.environment().put("TICKFORGE_DB_USER", DB.getUsername());
    p.environment().put("TICKFORGE_DB_PASSWORD", DB.getPassword());
    p.environment().putAll(env);
    p.redirectErrorStream(true);
    p.redirectOutput(dir.resolve(run + "-" + command + ".log").toFile());
    return p.start();
  }

  void success(String run, String command, String... extra) throws Exception {
    Process p = start(run, command, Map.of(), extra);
    try {
      assertTrue(p.waitFor(30, TimeUnit.SECONDS), "child timed out");
      assertEquals(
          0,
          p.exitValue(),
          () -> {
            try {
              return Files.readString(dir.resolve(run + "-" + command + ".log"));
            } catch (Exception e) {
              return e.toString();
            }
          });
    } finally {
      p.destroyForcibly();
    }
  }

  String report(String run) throws Exception {
    try (var r =
        new JdbcRunRepository(
            DriverManager.getConnection(DB.getJdbcUrl(), DB.getUsername(), DB.getPassword()),
            run)) {
      return r.canonicalReport();
    }
  }

  @Test
  void sigkillAtEveryTransactionBoundaryMatchesBaseline() throws Exception {
    success("baseline", "replay");
    String expected = report("baseline");
    for (String point :
        List.of("before-commit", "after-commit", "eof-before-commit", "eof-after-commit")) {
      String run = "kill-" + point;
      Path marker = dir.resolve(point + ".marker");
      Process child =
          start(
              run,
              "replay",
              Map.of(
                  "TICKFORGE_FAULT",
                  point,
                  "TICKFORGE_FAULT_INDEX",
                  "15",
                  "TICKFORGE_FAULT_MODE",
                  "barrier",
                  "TICKFORGE_FAULT_MARKER",
                  marker.toString()));
      try {
        assertTimeoutPreemptively(
            Duration.ofSeconds(20),
            () -> {
              while (!Files.exists(marker)) {
                assertTrue(child.isAlive(), "child exited before fault boundary");
                Thread.sleep(10);
              }
            });
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS));
      } finally {
        child.destroyForcibly();
      }
      success(run, "resume", "--speed", "1000");
      assertEquals(expected, report(run), point);
      success(run, "resume");
      assertEquals(expected, report(run), "complete resume no-op");
    }
  }

  @Test
  void droppedCommitAcknowledgementRecoversFromDatabaseCheckpoint() throws Exception {
    success("ack-baseline", "replay");
    try (var proxy = new CommitAckProxy(DB.getHost(), DB.getMappedPort(5432), 3)) {
      String url =
          "jdbc:postgresql://127.0.0.1:"
              + proxy.port()
              + "/"
              + DB.getDatabaseName()
              + "?sslmode=disable&preferQueryMode=simple";
      Process child = start("ambiguous", "replay", Map.of("TICKFORGE_DB_URL", url));
      try {
        assertTrue(child.waitFor(30, TimeUnit.SECONDS));
        assertNotEquals(0, child.exitValue());
        assertTrue(proxy.dropped.get());
      } finally {
        child.destroyForcibly();
      }
    }
    try (var repo =
        new JdbcRunRepository(
            DriverManager.getConnection(DB.getJdbcUrl(), DB.getUsername(), DB.getPassword()),
            "ambiguous")) {
      assertEquals(8, repo.load().nextIndex, "batch committed although acknowledgement was lost");
    }
    success("ambiguous", "resume");
    assertEquals(report("ack-baseline"), report("ambiguous"));
  }

  @Test
  void batchAndPacingDoNotChangeLedger() throws Exception {
    success("batch-base", "replay");

    success("paced", "replay", "--speed", "1000", "--batch-size", "100", "--queue-capacity", "4");
    assertEquals(report("batch-base"), report("paced"));
  }

  @Test
  void gracefulShutdownWriterExclusionAndChangedConfiguration() throws Exception {
    List<String> lines = Files.readAllLines(input);
    for (int i = 10; i < lines.size(); i++) {
      String[] fields = lines.get(i).split(",", -1);
      fields[1] = Long.toString(Long.parseLong(fields[1]) + 100000000000L);
      lines.set(i, String.join(",", fields));
    }
    Files.write(input, lines);
    Path manifestPath = Path.of(input + ".manifest.json");
    var original = Json.decode(Files.readString(manifestPath), DatasetManifest.class);
    Files.writeString(
        manifestPath,
        Json.encode(
            new DatasetManifest(
                original.formatVersion(),
                original.generatorVersion(),
                original.seed(),
                original.recordCount(),
                original.symbols(),
                original.priceScale(),
                DatasetManifest.hash(input),
                original.provenance())));
    success("shutdown-baseline", "replay");
    Process child = start("shutdown", "replay", Map.of(), "--speed", "1");
    try {
      var port = new java.util.concurrent.atomic.AtomicInteger();
      assertTimeoutPreemptively(
          Duration.ofSeconds(20),
          () -> {
            while (port.get() == 0) {
              assertTrue(child.isAlive());
              String log = Files.readString(dir.resolve("shutdown-replay.log"));
              if (log.contains("\n"))
                port.set(
                    Json.MAPPER
                        .readTree(log.lines().findFirst().orElseThrow())
                        .get("port")
                        .asInt());
              else Thread.sleep(10);
            }
            try (var client = java.net.http.HttpClient.newHttpClient()) {
              var request =
                  java.net.http.HttpRequest.newBuilder(
                          java.net.URI.create("http://127.0.0.1:" + port.get() + "/metrics.json"))
                      .build();
              while (true) {
                var response =
                    client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
                if (Json.MAPPER.readTree(response.body()).get("processedIndex").asLong() == 9)
                  break;
                assertTrue(child.isAlive());
                Thread.sleep(10);
              }
            }
          });
      Process second = start("shutdown", "replay", Map.of());
      try {
        assertTrue(second.waitFor(10, TimeUnit.SECONDS));
        assertNotEquals(0, second.exitValue());
        assertTrue(
            Files.readString(dir.resolve("shutdown-replay.log")).contains("already has a writer"));
      } finally {
        second.destroyForcibly();
      }
      child.destroy();
      assertTrue(child.waitFor(10, TimeUnit.SECONDS));
    } finally {
      child.destroyForcibly();
    }
    try (var repository =
        new JdbcRunRepository(
            DriverManager.getConnection(DB.getJdbcUrl(), DB.getUsername(), DB.getPassword()),
            "shutdown")) {
      var state = repository.load();
      assertFalse(state.finalized);
      assertEquals(
          10, state.nextIndex, "SIGTERM must commit processed rows 8 and 9 from the partial batch");
      assertEquals(9, Json.MAPPER.readTree(repository.canonicalReport()).get("outcomes").size());
    }
    Path changed = dir.resolve("changed.yaml");
    Files.writeString(
        changed, Files.readString(config).replace("feePerShare: 10", "feePerShare: 11"));
    Process incompatible = start("shutdown", "resume", Map.of(), "--config", changed.toString());
    try {
      assertTrue(incompatible.waitFor(10, TimeUnit.SECONDS));
      assertNotEquals(0, incompatible.exitValue());
    } finally {
      incompatible.destroyForcibly();
    }
    success("shutdown", "resume");
    assertEquals(report("shutdown-baseline"), report("shutdown"));
  }
}
