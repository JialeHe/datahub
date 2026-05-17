package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Session setup for SqlSetup DDL that uses unqualified PostgreSQL identifiers: ensures the feature
 * schema exists and sets {@code search_path} so {@code CREATE TABLE foo ...} resolves into the
 * intended schema (see {@link
 * com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties#normalizedPostgresSchema()}).
 */
public final class PostgresSqlSetupSession {

  private PostgresSqlSetupSession() {}

  /**
   * Runs {@code CREATE SCHEMA IF NOT EXISTS} for {@code schema}, then {@code SET search_path TO
   * schema, public} so subsequent statements may use unqualified names.
   *
   * <p>{@code schema} must already be validated as an unquoted identifier (see Postgres SqlSetup
   * properties normalization).
   */
  public static void ensureSchemaAndSearchPath(Connection connection, String schema)
      throws SQLException {
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
      st.execute("SET search_path TO " + schema + ", public");
    }
  }
}
