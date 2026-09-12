package dev.tickforge.persistence;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.domain.Ledger.*;
import dev.tickforge.engine.*;
import dev.tickforge.io.*;
import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

public final class JdbcRunRepository implements RunRepository {
  public static final String BUILD_ID = BuildIdentity.current();
  private final Connection connection;
  private final String runId;

  public record Metadata(
      String datasetHash, String configHash, String configJson, String inputPath, String buildId) {}

  public JdbcRunRepository(Connection connection, String runId) throws SQLException, IOException {
    this.connection = connection;
    this.runId = runId;
    try {
      if (!runId.matches("[A-Za-z0-9_-]{1,80}"))
        throw new IllegalArgumentException("invalid run ID");
      try (PreparedStatement p =
          connection.prepareStatement("SELECT pg_try_advisory_lock(hashtextextended(?,0))")) {
        p.setString(1, "tickforge:" + runId);
        try (ResultSet rs = p.executeQuery()) {
          rs.next();
          if (!rs.getBoolean(1)) throw new SQLException("run already has a writer");
        }
      }
      SchemaMigration.apply(connection);
    } catch (SQLException | IOException | RuntimeException e) {
      connection.close();
      throw e;
    }
  }

  public Metadata metadata() throws SQLException {
    try (PreparedStatement p =
        connection.prepareStatement(
            "SELECT dataset_hash,config_hash,config_json,input_path,build_id FROM runs WHERE run_id=?")) {
      p.setString(1, runId);
      try (ResultSet r = p.executeQuery()) {
        if (!r.next()) throw new IllegalArgumentException("unknown run: " + runId);
        return new Metadata(
            r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5));
      }
    }
  }

  public EngineState initialize(
      Path input, DatasetManifest manifest, RunConfig config, boolean resume) throws SQLException {
    if (resume) {
      Metadata m = metadata();
      if (!m.datasetHash.equals(manifest.sha256())
          || !m.configHash.equals(config.hash())
          || !m.buildId.equals(BUILD_ID))
        throw new IllegalArgumentException("resume input/config/build mismatch");
      return load();
    }
    connection.setAutoCommit(false);
    try {
      update(
          "INSERT INTO runs(run_id,dataset_hash,config_hash,config_json,input_path,build_id,status) VALUES(?,?,?,?::jsonb,?,?,'RUNNING')",
          runId,
          manifest.sha256(),
          config.hash(),
          Json.encode(config),
          input.toAbsolutePath().toString(),
          BUILD_ID);
      EngineState state = new EngineState(config.initialCash());
      persistState(state);
      connection.commit();
      return state;
    } catch (SQLException | RuntimeException e) {
      connection.rollback();
      throw e;
    } finally {
      connection.setAutoCommit(true);
    }
  }

  public EngineState load() throws SQLException {
    try (PreparedStatement p =
        connection.prepareStatement("SELECT state FROM checkpoints WHERE run_id=?")) {
      p.setString(1, runId);
      try (ResultSet r = p.executeQuery()) {
        if (!r.next()) throw new SQLException("missing checkpoint");
        EngineState state = Json.decode(r.getString(1), EngineState.class);
        if (state.version != 1) throw new SQLException("unsupported snapshot version");
        return state;
      }
    }
  }

  @Override
  public void commitBatch(BatchResult batch, EngineState state) throws SQLException {
    connection.setAutoCommit(false);
    try {
      for (Outcome e : batch.outcomes)
        update(
            "INSERT INTO event_outcomes VALUES(?,?,?,?,?,?)",
            runId,
            e.physicalIndex(),
            e.sequence(),
            e.type(),
            e.disposition(),
            e.reason());
      for (Order o : batch.orders.values())
        update(
            "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?::jsonb) ON CONFLICT(run_id,order_id) DO UPDATE SET filled=EXCLUDED.filled,status=EXCLUDED.status,data=EXCLUDED.data",
            runId,
            o.id(),
            o.symbol(),
            o.side().name(),
            o.quantity(),
            o.filled(),
            o.status().name(),
            o.triggerIndex(),
            Json.encode(o));
      for (OrderEvent e : batch.transitions)
        update(
            "INSERT INTO order_events VALUES(?,?,?,?::jsonb)",
            runId,
            e.orderId(),
            e.index(),
            Json.encode(e));
      for (Fill f : batch.fills)
        update(
            "INSERT INTO fills VALUES(?,?,?,?,?,?,?,?::jsonb)",
            runId,
            f.orderId(),
            f.index(),
            f.executionIndex(),
            f.price(),
            f.quantity(),
            f.fee(),
            Json.encode(f));
      persistState(state);
      fault("before-commit", state);
      connection.commit();
      fault("after-commit", state);
    } catch (SQLException | RuntimeException e) {
      try {
        connection.rollback();
      } catch (SQLException rollback) {
        e.addSuppressed(rollback);
      }
      throw e;
    } finally {
      connection.setAutoCommit(true);
    }
  }

  private void persistState(EngineState s) throws SQLException {
    for (var e : s.positions.entrySet())
      update(
          "INSERT INTO positions VALUES(?,?,?,?) ON CONFLICT(run_id,symbol) DO UPDATE SET quantity=EXCLUDED.quantity,cost_ticks=EXCLUDED.cost_ticks",
          runId,
          e.getKey(),
          e.getValue().quantity(),
          e.getValue().costTicks());
    update(
        "INSERT INTO account_state VALUES(?,?,?) ON CONFLICT(run_id) DO UPDATE SET cash_ticks=EXCLUDED.cash_ticks,fees_ticks=EXCLUDED.fees_ticks",
        runId,
        s.cash,
        s.fees);
    update(
        "INSERT INTO checkpoints VALUES(?,?,?,?::jsonb,?) ON CONFLICT(run_id) DO UPDATE SET next_index=EXCLUDED.next_index,state=EXCLUDED.state,finalized=EXCLUDED.finalized",
        runId,
        s.nextIndex,
        s.version,
        Json.encode(s),
        s.finalized);
    update("UPDATE runs SET status=? WHERE run_id=?", s.finalized ? "COMPLETE" : "RUNNING", runId);
  }

  private void update(String sql, Object... args) throws SQLException {
    try (PreparedStatement p = connection.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) p.setObject(i + 1, args[i]);
      p.executeUpdate();
    }
  }

  private static void fault(String point, EngineState state) {
    // Explicit process boundary hooks are opt-in and excluded from trading configuration.
    String requested = System.getenv("TICKFORGE_FAULT");
    if (point.equals(requested)
            && state.nextIndex
                >= Long.parseLong(System.getenv().getOrDefault("TICKFORGE_FAULT_INDEX", "2"))
        || ("eof-" + point).equals(requested) && state.finalized) Runtime.getRuntime().halt(86);
  }

  public String canonicalReport() throws SQLException {
    connection.setAutoCommit(false);
    connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    try {
      Map<String, Object> report = new TreeMap<>();
      report.put("state", load());
      report.put(
          "outcomes",
          rows(
              "SELECT physical_index,sequence,type,disposition,reason FROM event_outcomes WHERE run_id=? ORDER BY physical_index"));
      report.put(
          "orders",
          jsonRows("SELECT data FROM orders WHERE run_id=? ORDER BY trigger_index,order_id"));
      report.put(
          "transitions",
          jsonRows(
              "SELECT e.data FROM order_events e JOIN orders o USING(run_id,order_id) WHERE e.run_id=? ORDER BY o.trigger_index,e.transition_index"));
      report.put(
          "fills",
          jsonRows(
              "SELECT data FROM fills WHERE run_id=? ORDER BY execution_index,order_id,fill_index"));
      String result = Json.encode(report);
      connection.commit();
      return result;
    } catch (SQLException | RuntimeException e) {
      connection.rollback();
      throw e;
    } finally {
      connection.setAutoCommit(true);
    }
  }

  private List<Object> jsonRows(String sql) throws SQLException {
    List<Object> result = new ArrayList<>();
    try (PreparedStatement p = connection.prepareStatement(sql)) {
      p.setString(1, runId);
      try (ResultSet r = p.executeQuery()) {
        while (r.next()) result.add(Json.decode(r.getString(1), Object.class));
      }
    }
    return result;
  }

  private List<Object> rows(String sql) throws SQLException {
    List<Object> result = new ArrayList<>();
    try (PreparedStatement p = connection.prepareStatement(sql)) {
      p.setString(1, runId);
      try (ResultSet r = p.executeQuery()) {
        while (r.next()) {
          List<Object> row = new ArrayList<>();
          for (int i = 1; i <= r.getMetaData().getColumnCount(); i++) row.add(r.getObject(i));
          result.add(row);
        }
      }
    }
    return result;
  }

  @Override
  public void close() throws SQLException {
    connection.close();
  }
}
