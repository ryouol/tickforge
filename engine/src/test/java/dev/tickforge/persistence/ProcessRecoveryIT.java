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

    Process child =
        start(
            "paced",
            "replay",
            Map.of(),
            "--speed",
            "1000",
            "--batch-size",
            "100",
            "--queue-capacity",
            "4");
    try {
      assertTrue(child.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, child.exitValue());
    } finally {
      child.destroyForcibly();
    }
    assertEquals(report("batch-base"), report("paced"));
  }

  @Test
  void gracefulShutdownWriterExclusionAndChangedConfiguration() throws Exception {
    success("shutdown-baseline", "replay");
    Process child = start("shutdown", "replay", Map.of(), "--speed", "0.2");
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(20),
          () -> {
            while (true) {
              assertTrue(child.isAlive());
              try (Connection c =
                      DriverManager.getConnection(
                          DB.getJdbcUrl(), DB.getUsername(), DB.getPassword());
                  var statement =
                      c.prepareStatement(
                          "SELECT next_index FROM checkpoints WHERE run_id='shutdown'");
                  var result = statement.executeQuery()) {
                if (result.next() && result.getLong(1) >= 8) break;
              }
              Thread.sleep(20);
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
