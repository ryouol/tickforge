package dev.tickforge.persistence;

import static org.junit.jupiter.api.Assertions.*;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.engine.*;
import dev.tickforge.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;

@Testcontainers
class JdbcRunRepositoryIT {
  @Container
  static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:17.6-alpine");

  @TempDir Path dir;

  Connection connection() throws SQLException {
    return DriverManager.getConnection(DB.getJdbcUrl(), DB.getUsername(), DB.getPassword());
  }

  @Test
  void atomicSnapshotLockAndCompatibility() throws Exception {
    Path input = dir.resolve("input.csv");
    DatasetManifest.generate(input, 42, 10);
    var config =
        new RunConfig(
            1000000000,
            10,
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
    var manifest = DatasetManifest.verify(input, config.symbols().keySet());
    String report;
    try (var repo = new JdbcRunRepository(connection(), "atomic")) {
      var engine = new TradingEngine(config, repo.initialize(input, manifest, config, false));
      assertThrows(SQLException.class, () -> new JdbcRunRepository(connection(), "atomic"));
      try (var reader = new CsvEventReader(input, config.symbols().keySet())) {
        EventEnvelope e;
        while ((e = reader.next()) != null) engine.process(e);
      }
      engine.finish();
      repo.commitBatch(engine.batch(), engine.state());
      report = repo.canonicalReport();
      assertThrows(SQLException.class, () -> repo.commitBatch(engine.batch(), engine.state()));
      assertEquals(report, repo.canonicalReport());
    }
    try (var repo = new JdbcRunRepository(connection(), "atomic")) {
      assertTrue(repo.initialize(input, manifest, config, true).finalized);
      assertEquals(report, repo.canonicalReport());
      var changed =
          new DatasetManifest(1, "x", 42, 10, manifest.symbols(), 10000, "bad", "synthetic");
      assertThrows(
          IllegalArgumentException.class, () -> repo.initialize(input, changed, config, true));
    }
  }
}
