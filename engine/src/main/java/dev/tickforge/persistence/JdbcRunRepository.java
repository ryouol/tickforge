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
            "SELECT dataset_hash,config_hash,config_json,input_path,build_id FROM runs WHERE"
                + " run_id=?")) {
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
          "INSERT INTO runs(run_id,dataset_hash,config_hash,config_json,input_path,build_id,status)"
              + " VALUES(?,?,?,?::jsonb,?,?,'RUNNING')",
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
      insertBatch(
          "INSERT INTO event_outcomes VALUES(?,?,?,?,?,?)",
          batch.outcomes,
          e ->
              new Object[] {
                runId, e.physicalIndex(), e.sequence(), e.type(), e.disposition(), e.reason()
              });
      insertBatch(
          "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?::jsonb) ON CONFLICT(run_id,order_id) DO"
              + " UPDATE SET filled=EXCLUDED.filled,status=EXCLUDED.status,data=EXCLUDED.data",
          batch.orders.values(),
          o ->
              new Object[] {
                runId,
                o.id(),
                o.symbol(),
                o.side().name(),
                o.quantity(),
                o.filled(),
                o.status().name(),
                o.triggerIndex(),
                Json.encode(o)
              });
      insertBatch(
          "INSERT INTO order_events VALUES(?,?,?,?::jsonb)",
          batch.transitions,
          e -> new Object[] {runId, e.orderId(), e.index(), Json.encode(e)});
      insertBatch(
          "INSERT INTO fills VALUES(?,?,?,?,?,?,?,?::jsonb)",
          batch.fills,
          f ->
              new Object[] {
                runId,
                f.orderId(),
                f.index(),
                f.executionIndex(),
                f.price(),
                f.quantity(),
                f.fee(),
                Json.encode(f)
              });
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
          "INSERT INTO positions VALUES(?,?,?,?) ON CONFLICT(run_id,symbol) DO UPDATE SET"
              + " quantity=EXCLUDED.quantity,cost_ticks=EXCLUDED.cost_ticks WHERE"
              + " (positions.quantity,positions.cost_ticks) IS DISTINCT FROM"
              + " (EXCLUDED.quantity,EXCLUDED.cost_ticks)",
          runId,
          e.getKey(),
          e.getValue().quantity(),
          e.getValue().costTicks());
    update(
        "INSERT INTO account_state VALUES(?,?,?) ON CONFLICT(run_id) DO UPDATE SET"
            + " cash_ticks=EXCLUDED.cash_ticks,fees_ticks=EXCLUDED.fees_ticks WHERE"
            + " (account_state.cash_ticks,account_state.fees_ticks) IS DISTINCT FROM"
            + " (EXCLUDED.cash_ticks,EXCLUDED.fees_ticks)",
        runId,
        s.cash,
        s.fees);
    update(
        "INSERT INTO checkpoints VALUES(?,?,?,?::jsonb,?) ON CONFLICT(run_id) DO UPDATE SET"
            + " next_index=EXCLUDED.next_index,state=EXCLUDED.state,finalized=EXCLUDED.finalized",
        runId,
        s.nextIndex,
        s.version,
        Json.encode(s),
        s.finalized);
    if (s.finalized)
      update("UPDATE runs SET status='COMPLETE' WHERE run_id=? AND status<>'COMPLETE'", runId);
  }

  private static void bind(PreparedStatement statement, Object[] args) throws SQLException {
    for (int i = 0; i < args.length; i++) statement.setObject(i + 1, args[i]);
  }

  private <T> void insertBatch(
      String sql, Collection<T> values, java.util.function.Function<T, Object[]> parameters)
      throws SQLException {
    if (values.isEmpty()) return;
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (T value : values) {
        bind(statement, parameters.apply(value));
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  private void update(String sql, Object... args) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      bind(statement, args);
      statement.executeUpdate();
    }
  }

  private static void fault(String point, EngineState state) {
    // Explicit process boundary hooks are opt-in and excluded from trading configuration.
    String requested = System.getenv("TICKFORGE_FAULT");
    if (point.equals(requested)
            && state.nextIndex
                >= Long.parseLong(System.getenv().getOrDefault("TICKFORGE_FAULT_INDEX", "2"))
        || ("eof-" + point).equals(requested) && state.finalized) {
      if ("barrier".equals(System.getenv("TICKFORGE_FAULT_MODE"))) {
        try {
          java.nio.file.Files.writeString(Path.of(System.getenv("TICKFORGE_FAULT_MARKER")), point);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        while (true) java.util.concurrent.locks.LockSupport.parkNanos(10000000);
      }
      Runtime.getRuntime().halt(86);
    }
  }

  public String canonicalReport() throws SQLException, IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeCanonicalReport(out);
    return out.toString(java.nio.charset.StandardCharsets.UTF_8);
  }

  public void writeCanonicalReport(OutputStream output) throws SQLException, IOException {
    connection.setAutoCommit(false);
    connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    try (var json = Json.MAPPER.getFactory().createGenerator(output)) {
      json.disable(com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      json.writeStartObject();
      writeRows(
          json, "account", "SELECT cash_ticks,fees_ticks FROM account_state WHERE run_id=?", false);
      writeRows(
          json,
          "fills",
          "SELECT data FROM fills WHERE run_id=? ORDER BY execution_index,order_id,fill_index",
          true);
      writeRows(
          json,
          "orders",
          "SELECT data FROM orders WHERE run_id=? ORDER BY trigger_index,order_id",
          true);
      writeRows(
          json,
          "outcomes",
          "SELECT physical_index,sequence,type,disposition,reason FROM event_outcomes WHERE"
              + " run_id=? ORDER BY physical_index",
          false);
      writeRows(
          json,
          "positions",
          "SELECT symbol,quantity,cost_ticks FROM positions WHERE run_id=? ORDER BY symbol",
          false);
      json.writeObjectField("state", load());
      writeRows(
          json,
          "transitions",
          "SELECT e.data FROM order_events e JOIN orders o USING(run_id,order_id) WHERE e.run_id=?"
              + " ORDER BY o.trigger_index,e.transition_index",
          true);
      json.writeEndObject();
      json.flush();
      connection.commit();
    } catch (SQLException | IOException | RuntimeException e) {
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

  private void writeRows(
      com.fasterxml.jackson.core.JsonGenerator json, String name, String sql, boolean encodedJson)
      throws SQLException, IOException {
    json.writeArrayFieldStart(name);
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, runId);
      statement.setFetchSize(1000);
      try (ResultSet result = statement.executeQuery()) {
        int columns = result.getMetaData().getColumnCount();
        while (result.next()) {
          if (encodedJson) json.writeObject(Json.decode(result.getString(1), Object.class));
          else {
            json.writeStartArray();
            for (int i = 1; i <= columns; i++) json.writeObject(result.getObject(i));
            json.writeEndArray();
          }
        }
      }
    }
    json.writeEndArray();
  }

  @Override
  public void close() throws SQLException {
    connection.close();
  }
}
