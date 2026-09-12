package dev.tickforge.persistence;

import dev.tickforge.io.DatasetManifest;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public final class SchemaMigration {
  private SchemaMigration() {}

  public static void apply(Connection connection) throws SQLException, IOException {
    try (InputStream in =
        SchemaMigration.class.getResourceAsStream("/db/migration/V1__ledger.sql")) {
      if (in == null) throw new IOException("missing migration");
      String sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      String hash = DatasetManifest.hashText(sql);
      connection.setAutoCommit(false);
      try (Statement s = connection.createStatement()) {
        s.execute("SELECT pg_advisory_xact_lock(742139601)");
        s.execute(
            "CREATE TABLE IF NOT EXISTS schema_version(version integer PRIMARY KEY, checksum text NOT NULL)");
        try (ResultSet rs = s.executeQuery("SELECT version,checksum FROM schema_version")) {
          if (rs.next()) {
            if (rs.getInt(1) != 1 || !hash.equals(rs.getString(2)) || rs.next())
              throw new SQLException("incompatible schema migration");
          } else {
            s.execute(sql);
            try (PreparedStatement p =
                connection.prepareStatement("INSERT INTO schema_version VALUES(1,?)")) {
              p.setString(1, hash);
              p.executeUpdate();
            }
          }
        }
        connection.commit();
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      } finally {
        connection.setAutoCommit(true);
      }
    }
  }
}
