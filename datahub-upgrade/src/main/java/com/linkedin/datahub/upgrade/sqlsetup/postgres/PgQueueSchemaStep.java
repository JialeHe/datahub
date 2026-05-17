package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.postgres.PgQueueResolvedTopicCatalogEntry;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

/**
 * Applies PostgreSQL DDL for the DataHub native queue store (at-least-once; requires {@code
 * pg_partman} for the message table; optional {@code pg_cron} when listed in {@code
 * pg_available_extensions} and maintenance cron is enabled).
 */
@Slf4j
@RequiredArgsConstructor
public class PgQueueSchemaStep implements UpgradeStep {

  /**
   * Allowlisted optional extensions per connection scope ({@code pg_cron} installs on the registry
   * DB).
   */
  private static final Set<String> PGQUEUE_PARTMAN_EXTENSIONS = Set.of("pg_partman");

  private static final Set<String> PGQUEUE_CRON_EXTENSIONS = Set.of("pg_cron");

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;
  @Nullable private final KafkaConfiguration kafkaConfiguration;

  @Override
  public String id() {
    return "PgQueueSchemaStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Applying PostgreSQL pgQueue schema...");
        PgQueueSetupOptions q = postgresProperties.buildPgQueueOptions(kafkaConfiguration);
        if (q == null) {
          String msg = "pgQueue is enabled but PgQueueSetupOptions is null.";
          log.error(msg);
          context.report().addLine(msg);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        String schema = q.getSchema();
        String cronSchema = postgresProperties.normalizedPgCronSchema();

        try (Connection connection = server.dataSource().getConnection()) {
          connection.setAutoCommit(true);

          maybeCreateExtension(connection, "pg_partman", true, PGQUEUE_PARTMAN_EXTENSIONS);
          if (!isExtensionInstalled(connection, "pg_partman")) {
            String msg =
                "pgQueue SqlSetup requires pg_partman but it is not installed. "
                    + "Install the extension (it must appear in pg_available_extensions).";
            log.error(msg);
            context.report().addLine(msg);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          executeClasspathSql(connection, "sqlsetup/pgqueue/init_pgqueue_00_extensions.sql");

          PostgresSqlSetupSession.ensureSchemaAndSearchPath(connection, schema);

          String tablePrefix = q.getTablePrefix();
          String schemaSql =
              loadClasspathSql("sqlsetup/pgqueue/init_pgqueue_01_schema_partman.sql")
                  .replace("__PGQUEUE_PREFIX__", tablePrefix)
                  .replace("__PGQUEUE_SCHEMA__", quotePgIdentifier(schema));
          executeSql(connection, schemaSql);

          String leaseSql =
              loadClasspathSql("sqlsetup/pgqueue/init_pgqueue_04_message_group_lease.sql")
                  .replace("__PGQUEUE_PREFIX__", tablePrefix)
                  .replace("__PGQUEUE_SCHEMA__", quotePgIdentifier(schema));
          executeSql(connection, leaseSql);

          String consumerRegSql =
              loadClasspathSql("sqlsetup/pgqueue/init_pgqueue_05_consumer_registration.sql")
                  .replace("__PGQUEUE_PREFIX__", tablePrefix)
                  .replace("__PGQUEUE_SCHEMA__", quotePgIdentifier(schema));
          executeSql(connection, consumerRegSql);

          String dropMessageRowLocksSql =
              loadClasspathSql("sqlsetup/pgqueue/init_pgqueue_06_drop_message_row_locks.sql")
                  .replace("__PGQUEUE_PREFIX__", tablePrefix)
                  .replace("__PGQUEUE_SCHEMA__", quotePgIdentifier(schema));
          executeSql(connection, dropMessageRowLocksSql);

          upsertPgQueueTopicCatalog(connection, schema, q);

          String partmanExtensionSchema = resolvePgPartmanExtensionSchema(connection);
          if (partmanExtensionSchema == null || partmanExtensionSchema.isBlank()) {
            String msg =
                "pg_partman is installed but its extension schema could not be read from"
                    + " pg_extension / pg_namespace.";
            log.error(msg);
            context.report().addLine(msg);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
          int maxTopicRetention =
              queryMaxTopicRetentionMaxAgeSeconds(connection, schema, tablePrefix);
          @Nullable
          String partmanRetentionIntervalText =
              PostgresSqlSetupProperties.resolvePartmanPartitionRetentionIntervalText(
                  q.getTopicDefaultRetentionMaxAgeSeconds(),
                  maxTopicRetention,
                  q.getPartmanPartitionInterval());

          String partmanTail =
              "    PERFORM "
                  + quotePgIdentifier(partmanExtensionSchema)
                  + ".run_maintenance('"
                  + schema.replace("'", "''")
                  + "."
                  + tablePrefix
                  + "_message');\n";
          String maintenanceSql =
              loadClasspathSql("sqlsetup/pgqueue/init_pgqueue_02_maintenance.sql")
                  .replace("__PGQUEUE_PREFIX__", tablePrefix)
                  .replace("__PGQUEUE_SCHEMA__", quotePgIdentifier(schema))
                  .replace(
                      "__BATCH_DELETE_LIMIT__",
                      Integer.toString(q.getMaintenanceBatchDeleteLimit()))
                  .replace("__PGQUEUE_APPLY_RETENTION_PARTMAN_TAIL__", partmanTail);
          executeSql(connection, maintenanceSql);

          String qualMessage = schema + "." + tablePrefix + "_message";
          String partmanSql =
              loadClasspathSql("sqlsetup/pgqueue/init_pgqueue_03_partman.sql")
                  .replace("__PARTMAN_PARENT_QUALIFIED__", qualMessage)
                  .replace(
                      "__PARTMAN_INTERVAL__",
                      sanitizePartmanIntervalLiteral(q.getPartmanPartitionInterval()))
                  .replace("__PARTMAN_PREMAKE__", Integer.toString(q.getPartmanPremake()));
          executeSql(connection, partmanSql);
          String retentionUpdateSql =
              partmanRetentionUpdateSql(
                  partmanExtensionSchema, schema, partmanRetentionIntervalText, tablePrefix);
          if (!retentionUpdateSql.isEmpty()) {
            executeSql(connection, retentionUpdateSql);
          }

          if (q.isMaintenanceCronEnabled()) {
            String jobDb = connection.getCatalog();
            try (Connection cronConn = PgCronAdminConnections.open(postgresProperties)) {
              maybeCreateExtension(cronConn, "pg_cron", true, PGQUEUE_CRON_EXTENSIONS);
              registerQueueRetentionCronJob(
                  cronConn,
                  cronSchema,
                  schema,
                  tablePrefix,
                  q.getMaintenanceIntervalSeconds(),
                  jobDb);
            }
          }
        }

        context.report().addLine("pgQueue schema applied successfully.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("PgQueueSchemaStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private static void maybeCreateExtension(
      Connection connection, String extensionName, boolean want, Set<String> allowedNames)
      throws SQLException {
    if (!want) {
      return;
    }
    if (!allowedNames.contains(extensionName)) {
      throw new IllegalArgumentException("Unsupported extension name: " + extensionName);
    }
    if (!isExtensionAvailable(connection, extensionName)) {
      log.warn(
          "Extension {} is not listed in pg_available_extensions; skipping CREATE EXTENSION.",
          extensionName);
      return;
    }
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE EXTENSION IF NOT EXISTS " + extensionName);
      log.info("CREATE EXTENSION IF NOT EXISTS {} attempted.", extensionName);
    } catch (SQLException e) {
      log.warn(
          "CREATE EXTENSION {} skipped or failed (non-fatal for SqlSetup): {}",
          extensionName,
          e.getMessage());
    }
  }

  private static boolean isExtensionAvailable(Connection connection, String extensionName)
      throws SQLException {
    String safe = extensionName.replace("'", "''");
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT 1 FROM pg_available_extensions WHERE name = '" + safe + "' LIMIT 1")) {
      return rs.next();
    }
  }

  /**
   * Largest {@code retention_max_age_seconds} across queue topics (0 if the table is missing or
   * empty).
   */
  static int queryMaxTopicRetentionMaxAgeSeconds(
      Connection connection, String schema, String tablePrefix) throws SQLException {
    String qualified = quotePgIdentifier(schema) + "." + quotePgIdentifier(tablePrefix + "_topic");
    String sql = "SELECT COALESCE(MAX(retention_max_age_seconds), 0) FROM " + qualified;
    try (Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }

  /**
   * Seeds/merges catalog rows from configuration. {@code partition_count} uses {@code GREATEST} on
   * conflict so it never drops below existing catalog or below {@code MAX(partition_id)+1} for
   * queued messages (avoids orphan partitions when Kafka-derived defaults shrink).
   */
  static void upsertPgQueueTopicCatalog(Connection connection, String schema, PgQueueSetupOptions q)
      throws SQLException {
    if (q.getResolvedTopicCatalog().isEmpty()) {
      return;
    }
    String qualified =
        quotePgIdentifier(schema) + "." + quotePgIdentifier(q.getTablePrefix() + "_topic");
    String qualifiedMessage =
        quotePgIdentifier(schema) + "." + quotePgIdentifier(q.getTablePrefix() + "_message");
    String qualifiedContentType =
        quotePgIdentifier(schema) + "." + quotePgIdentifier(q.getTablePrefix() + "_content_type");
    String sql =
        "INSERT INTO "
            + qualified
            + " AS ptopic (topic_name, partition_count, retention_max_age_seconds, "
            + "max_rows_per_topic, max_total_payload_bytes, default_content_type_id, aggressive_retention) "
            + "VALUES (?,?,?,?,?,(SELECT id FROM "
            + qualifiedContentType
            + " WHERE mime = ? LIMIT 1),?) ON CONFLICT (topic_name) DO UPDATE SET "
            + "partition_count = GREATEST(1, EXCLUDED.partition_count, ptopic.partition_count, "
            + "COALESCE((SELECT MAX(m.partition_id) FROM "
            + qualifiedMessage
            + " m WHERE m.topic_id = ptopic.id), -1) + 1), "
            + "retention_max_age_seconds = EXCLUDED.retention_max_age_seconds, "
            + "max_rows_per_topic = EXCLUDED.max_rows_per_topic, "
            + "max_total_payload_bytes = EXCLUDED.max_total_payload_bytes, "
            + "default_content_type_id = EXCLUDED.default_content_type_id, "
            + "aggressive_retention = EXCLUDED.aggressive_retention";
    for (PgQueueResolvedTopicCatalogEntry e : q.getResolvedTopicCatalog()) {
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, e.getTopicName());
        ps.setInt(2, e.getPartitionCount());
        ps.setInt(3, e.getRetentionMaxAgeSeconds());
        ps.setLong(4, e.getMaxRowsPerTopic());
        ps.setLong(5, e.getMaxTotalPayloadBytesPerTopic());
        ps.setString(6, q.getTopicDefaultContentTypeMime());
        ps.setBoolean(7, e.isAggressiveRetention());
        ps.executeUpdate();
      }
      log.info(
          "pgQueue topic catalog upsert: {} -> retention_max_age_seconds={}",
          e.getTopicName(),
          e.getRetentionMaxAgeSeconds());
    }
  }

  private static boolean isExtensionInstalled(Connection connection, String extensionName)
      throws SQLException {
    String safe = extensionName.replace("'", "''");
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT 1 FROM pg_extension WHERE extname = '" + safe + "' LIMIT 1")) {
      return rs.next();
    }
  }

  private static void registerQueueRetentionCronJob(
      Connection cronConnection,
      String cronSchema,
      String applicationSchema,
      String tablePrefix,
      int intervalSeconds,
      String jobTargetDatabase)
      throws SQLException {
    if (jobTargetDatabase == null || jobTargetDatabase.isBlank()) {
      log.error(
          "Cannot register pgQueue pg_cron job: JDBC catalog (database name) is empty; "
              + "fix the entity store JDBC URL / connection.");
      return;
    }
    String jobName =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGQUEUE_CRON_ROLE, jobTargetDatabase, applicationSchema, tablePrefix);
    String schedule = toPgCronSchedule(intervalSeconds);
    if (!PgCronMaintenance.isExtensionInstalled(cronConnection, "pg_cron")) {
      log.warn(
          "pg_cron is not installed; skipping in-database schedule for job {}. "
              + "Use postgres.pgQueue.maintenance.cronEnabled=false or install pg_cron so it appears in pg_available_extensions.",
          jobName);
      return;
    }
    String command = "SELECT " + applicationSchema + "." + tablePrefix + "_apply_retention()";
    PgCronMaintenance.replaceCronJobInDatabase(
        cronConnection, cronSchema, jobName, schedule, command, jobTargetDatabase);
    log.info(
        "Registered pg_cron job {} with schedule '{}' for {} (target database {})",
        jobName,
        schedule,
        command,
        jobTargetDatabase);
  }

  static String partmanRetentionUpdateSql(
      String partmanExtensionSchema,
      String schema,
      @Nullable String partmanRetentionIntervalText,
      String tablePrefix) {
    if (partmanRetentionIntervalText == null || partmanRetentionIntervalText.isEmpty()) {
      return "";
    }
    String escRetention = partmanRetentionIntervalText.replace("'", "''");
    String escSchema = schema.replace("'", "''");
    return "  UPDATE "
        + quotePgIdentifier(partmanExtensionSchema)
        + ".part_config\n"
        + "  SET retention = '"
        + escRetention
        + "',\n"
        + "      retention_keep_table = false,\n"
        + "      retention_keep_index = false\n"
        + "  WHERE parent_table = '"
        + escSchema
        + "."
        + tablePrefix
        + "_message';\n";
  }

  /**
   * Schema that owns pg_partman objects ({@code pg_extension.extnamespace}), for example {@code
   * public} when the extension was created without {@code SCHEMA partman}.
   */
  @Nullable
  static String resolvePgPartmanExtensionSchema(Connection connection) throws SQLException {
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT n.nspname FROM pg_extension e "
                    + "JOIN pg_namespace n ON n.oid = e.extnamespace "
                    + "WHERE e.extname = 'pg_partman' LIMIT 1")) {
      if (!rs.next()) {
        return null;
      }
      return rs.getString(1);
    }
  }

  static String quotePgIdentifier(String ident) {
    if (ident == null || ident.isEmpty()) {
      throw new IllegalArgumentException("PostgreSQL identifier required");
    }
    StringBuilder sb = new StringBuilder();
    sb.append('"');
    for (int i = 0; i < ident.length(); i++) {
      char c = ident.charAt(i);
      if (c == '"') {
        sb.append("\"\"");
      } else {
        sb.append(c);
      }
    }
    sb.append('"');
    return sb.toString();
  }

  /**
   * {@code q.getPartmanPartitionInterval()} is allowlisted in {@link
   * com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties}; this only escapes quotes for
   * safe embedding in SQL text literals.
   */
  static String sanitizePartmanIntervalLiteral(String partmanPartitionInterval) {
    return partmanPartitionInterval.replace("'", "''");
  }

  /** Maps intervalSeconds to a pg_cron schedule (minute/hour granularity). */
  public static String toPgCronSchedule(int intervalSeconds) {
    int sec = Math.max(60, intervalSeconds);
    if (sec % 86400 == 0) {
      int days = sec / 86400;
      days = Math.max(1, Math.min(31, days));
      return days == 1 ? "0 0 * * *" : ("0 0 */" + days + " * *");
    }
    if (sec % 3600 == 0) {
      int hours = Math.max(1, Math.min(23, sec / 3600));
      return "0 */" + hours + " * * *";
    }
    int minutes = Math.max(1, Math.min(59, sec / 60));
    return "*/" + minutes + " * * * *";
  }

  private static void executeClasspathSql(Connection connection, String classpathLocation)
      throws Exception {
    String sql = loadClasspathSql(classpathLocation);
    if (sql.isBlank()) {
      return;
    }
    executeSql(connection, sql);
  }

  private static String loadClasspathSql(String classpathLocation) throws Exception {
    ClassPathResource resource = new ClassPathResource(classpathLocation);
    return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
  }

  private static void executeSql(Connection connection, String sql) throws Exception {
    if (sql.isEmpty()) {
      return;
    }
    try (Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(sql);
      for (; ; ) {
        if (hasResultSet) {
          try (ResultSet rs = statement.getResultSet()) {
            while (rs.next()) {
              // drain
            }
          }
        } else if (statement.getUpdateCount() == -1) {
          break;
        }
        hasResultSet = statement.getMoreResults();
      }
    }
  }
}
