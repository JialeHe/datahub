package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import org.springframework.core.io.ClassPathResource;
import org.testng.annotations.Test;

public class PgQueueSchemaStepTest {

  @Test
  public void testToPgCronScheduleHourly() {
    assertEquals(PgQueueSchemaStep.toPgCronSchedule(3600), "0 */1 * * *");
  }

  @Test
  public void testToPgCronScheduleEveryTwoHours() {
    assertEquals(PgQueueSchemaStep.toPgCronSchedule(7200), "0 */2 * * *");
  }

  @Test
  public void testToPgCronScheduleMinuteGranularity() {
    String s = PgQueueSchemaStep.toPgCronSchedule(300);
    assertTrue(s.startsWith("*/"));
    assertTrue(s.endsWith(" * * * *"));
  }

  @Test
  public void testToPgCronScheduleClampsBelowSixtySeconds() {
    assertEquals(PgQueueSchemaStep.toPgCronSchedule(30), "*/1 * * * *");
  }

  @Test
  public void testPartmanRetentionUpdateSql() {
    String sql =
        PgQueueSchemaStep.partmanRetentionUpdateSql("partman", "datahub", "30 days", "pgqueue");
    assertTrue(sql.contains("UPDATE \"partman\".part_config"));
    assertTrue(sql.contains("'30 days'"));
    assertTrue(sql.contains("datahub.pgqueue_message"));
  }

  @Test
  public void testPartmanRetentionUpdateSqlUsesResolvedPublicSchema() {
    String sql =
        PgQueueSchemaStep.partmanRetentionUpdateSql("public", "datahub", "7 days", "pgqueue");
    assertTrue(sql.contains("UPDATE \"public\".part_config"));
  }

  @Test
  public void testPartmanRetentionUpdateSqlEmptyWhenNoRetention() {
    assertEquals(
        PgQueueSchemaStep.partmanRetentionUpdateSql("partman", "datahub", null, "pgqueue"), "");
    assertEquals(
        PgQueueSchemaStep.partmanRetentionUpdateSql("partman", "datahub", "", "pgqueue"), "");
  }

  @Test
  public void testSanitizePartmanIntervalLiteralEscapesQuotes() {
    assertEquals(PgQueueSchemaStep.sanitizePartmanIntervalLiteral("1 day"), "1 day");
    assertEquals(PgQueueSchemaStep.sanitizePartmanIntervalLiteral("1'day"), "1''day");
  }

  /**
   * Regression for "relation \"metadata_queue_topic\" does not exist" when the trigger fires from
   * an application JDBC connection that doesn't have the queue schema in its search_path. The
   * trigger function MUST pin its own search_path so it resolves unqualified table references.
   */
  @Test
  public void testSchemaPartmanSqlSubstitutesPgQueueSchemaToken() throws Exception {
    String raw =
        new String(
                new ClassPathResource("sqlsetup/pgqueue/init_pgqueue_01_schema_partman.sql")
                    .getInputStream()
                    .readAllBytes(),
                StandardCharsets.UTF_8)
            .trim();

    String tablePrefix = "metadata_queue";
    String schema = "queue";
    String substituted =
        raw.replace("__PGQUEUE_PREFIX__", tablePrefix)
            .replace("__PGQUEUE_SCHEMA__", PgQueueSchemaStep.quotePgIdentifier(schema));

    assertFalse(
        substituted.contains("__PGQUEUE_PREFIX__"),
        "Unsubstituted prefix token remains: " + substituted);
    assertFalse(
        substituted.contains("__PGQUEUE_SCHEMA__"),
        "Unsubstituted schema token remains: " + substituted);
    assertTrue(
        substituted.contains("CHECK (priority BETWEEN 0 AND 9)"),
        "Priority range CHECK constraint missing after substitution: " + substituted);
  }
}
