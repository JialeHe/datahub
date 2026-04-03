package com.linkedin.metadata.kafka.hook.ingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionMetricsHookTest {

  private static final String ASPECT_NAME = "dataHubExecutionRequestResult";
  private static final String ENTITY_TYPE = "dataHubExecutionRequest";
  private static final String TEST_EXECUTION_REQUEST_URN =
      "urn:li:dataHubExecutionRequest:test-pipeline-12345";

  private IngestionMetricsHook hook;
  private SimpleMeterRegistry meterRegistry;

  @BeforeMethod
  public void setup() {
    meterRegistry = new SimpleMeterRegistry();
    // Pass null for entityClient in tests — no entity client RPCs in the current implementation
    hook = new IngestionMetricsHook(meterRegistry, true);
    hook.init(TestOperationContexts.systemContextNoSearchAuthorization());
  }

  @Test
  public void testInvokeRecordsMetrics() throws Exception {
    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 2, 3);

    hook.invoke(event);

    // Verify run counter
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);

    // Verify duration was recorded
    DistributionSummary durationSummary =
        meterRegistry.find("com.datahub.ingest.duration_ms").summary();
    assertNotNull(durationSummary);
    assertEquals(durationSummary.totalAmount(), 1000.0);

    // Verify events_produced counter
    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertNotNull(eventsCounter);
    assertEquals(eventsCounter.count(), 100.0);

    // Verify records_written counter
    Counter recordsCounter = meterRegistry.find("com.datahub.ingest.records_written").counter();
    assertNotNull(recordsCounter);
    assertEquals(recordsCounter.count(), 5.0);

    // Verify warnings counter
    Counter warningsCounter = meterRegistry.find("com.datahub.ingest.warnings").counter();
    assertNotNull(warningsCounter);
    assertEquals(warningsCounter.count(), 2.0);

    // Verify failures counter
    Counter failuresCounter = meterRegistry.find("com.datahub.ingest.failures").counter();
    assertNotNull(failuresCounter);
    assertEquals(failuresCounter.count(), 3.0);

    // Verify platform tag extracted from CLI format (Source (file) → platform=file)
    Counter platformCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("platform", "file").counter();
    assertNotNull(platformCounter);
    assertEquals(platformCounter.count(), 1.0);
  }

  @Test
  public void testInvokeSkipsWhenDisabled() throws Exception {
    IngestionMetricsHook disabledHook = new IngestionMetricsHook(meterRegistry, false);
    disabledHook.init(TestOperationContexts.systemContextNoSearchAuthorization());

    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);

    disabledHook.invoke(event);

    // Verify no metrics recorded
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testInvokeWithCreateChangeType() throws Exception {
    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);
    event.setChangeType(ChangeType.CREATE);

    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);
  }

  @Test
  public void testInvokeWithMalformedJsonDoesNotThrow() throws Exception {
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue("{invalid json{{");
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    // Should not throw — parseStructuredReport returns null, hook warns and returns
    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testInvokeSkipsNullAspect() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));
    // aspect is null — not set

    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testInvokeSkipsNullEntityUrn() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(new ExecutionRequestResult()));
    // entityUrn is null — not set

    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testNullStatusDefaultsToUnknown() throws Exception {
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus(""); // Explicitly set empty status to test defaulting
    result.setDurationMs(1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue("{}");
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    hook.invoke(event);

    Counter runsCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("status", "unknown").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);
  }

  @Test
  public void testInvokeSkipsNonIngestionReport() throws Exception {
    // Create an execution result with a non-ingestion report type
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("TEST_CONNECTION"); // Not CLI_INGEST or RUN_INGEST
    structuredReport.setSerializedValue("{}");
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    hook.invoke(event);

    // Verify no metrics recorded for non-ingestion report
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testInvokeWithRunIngestReportType() throws Exception {
    // Test executor format (RUN_INGEST) which uses different JSON structure
    MetadataChangeLog event = createExecutorTestEvent("SUCCEEDED", 2000L, 500, 50, 1, 0);

    hook.invoke(event);

    // Verify run counter
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);

    // Verify duration was recorded
    DistributionSummary durationSummary =
        meterRegistry.find("com.datahub.ingest.duration_ms").summary();
    assertNotNull(durationSummary);
    assertEquals(durationSummary.totalAmount(), 2000.0);

    // Verify events_produced counter
    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertNotNull(eventsCounter);
    assertEquals(eventsCounter.count(), 500.0);

    // Verify records_written counter
    Counter recordsCounter = meterRegistry.find("com.datahub.ingest.records_written").counter();
    assertNotNull(recordsCounter);
    assertEquals(recordsCounter.count(), 50.0);

    // Verify warnings counter
    Counter warningsCounter = meterRegistry.find("com.datahub.ingest.warnings").counter();
    assertNotNull(warningsCounter);
    assertEquals(warningsCounter.count(), 1.0);

    // Verify platform tag is extracted correctly
    Counter platformCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("platform", "snowflake").counter();
    assertNotNull(platformCounter);
    assertEquals(platformCounter.count(), 1.0);
  }

  @Test
  public void testInvokeWithDifferentStatuses() throws Exception {
    // Test with FAILURE status
    MetadataChangeLog failedEvent = createTestEvent("FAILURE", 500L, 50, 0, 0, 5);
    hook.invoke(failedEvent);

    Counter failedRunsCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("status", "FAILURE").counter();
    assertNotNull(failedRunsCounter);
    assertEquals(failedRunsCounter.count(), 1.0);

    // Test with SUCCEEDED status
    MetadataChangeLog successEvent = createTestEvent("SUCCEEDED", 1000L, 100, 10, 0, 0);
    hook.invoke(successEvent);

    Counter succeededRunsCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("status", "SUCCEEDED").counter();
    assertNotNull(succeededRunsCounter);
    assertEquals(succeededRunsCounter.count(), 1.0);
  }

  @Test
  public void testMultipleInvocations() throws Exception {
    // First invocation
    MetadataChangeLog event1 = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);
    hook.invoke(event1);

    // Second invocation
    MetadataChangeLog event2 = createTestEvent("SUCCEEDED", 2000L, 200, 10, 0, 0);
    hook.invoke(event2);

    // Verify counters accumulated
    Counter runsCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("status", "SUCCEEDED").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 2.0);

    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertNotNull(eventsCounter);
    assertEquals(eventsCounter.count(), 300.0); // 100 + 200
  }

  @Test
  public void testInvokeWithMinimalData() throws Exception {
    // Create event with minimal data (no optional metrics)
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    // No duration set

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue("{}"); // Empty report
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    // Should not throw
    hook.invoke(event);

    // Verify run counter was still recorded
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);
  }

  @Test
  public void testSinkFailuresRecorded() throws Exception {
    String reportJson =
        "{"
            + "\"cli\": {\"cli_version\": \"0.14.0\"},"
            + "\"Source (file)\": {"
            + "  \"events_produced\": 100,"
            + "  \"warnings\": [],"
            + "  \"failures\": [],"
            + "  \"platform\": \"file\""
            + "},"
            + "\"Sink (datahub-rest)\": {"
            + "  \"total_records_written\": 90,"
            + "  \"failures\": [\"write failed\",\"timeout\"]"
            + "}"
            + "}";

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);
    result.setStartTimeMs(System.currentTimeMillis() - 1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    hook.invoke(event);

    Counter sinkFailuresCounter = meterRegistry.find("com.datahub.ingest.sink_failures").counter();
    assertNotNull(sinkFailuresCounter);
    assertEquals(sinkFailuresCounter.count(), 2.0);
  }

  @Test
  public void testExecutorFormatProcessed() throws Exception {
    // Executor format (RUN_INGEST) is processed correctly — version is log-only, not a metric label
    MetadataChangeLog event = createExecutorTestEvent("SUCCEEDED", 2000L, 500, 50, 0, 0);

    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);
    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertNotNull(eventsCounter);
    assertEquals(eventsCounter.count(), 500.0);
  }

  @Test
  public void testCliFormatProcessed() throws Exception {
    // CLI format (CLI_INGEST) is processed correctly — version is log-only, not a metric label
    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);

    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);
  }

  @Test
  public void testSourceReportMissingNestedReportReturnsZeroCounts() throws Exception {
    // Executor format with source present but no nested source.report — previously returned
    // the source parent node, causing all numeric lookups to silently return 0.
    // Now correctly returns missingNode so metrics reflect the data actually available.
    String reportJson =
        "{"
            + "\"cli\": {\"cli_version\": \"1.3.1\"},"
            + "\"source\": {"
            + "  \"type\": \"snowflake\""
            + "},"
            + "\"sink\": {"
            + "  \"type\": \"datahub-rest\","
            + "  \"report\": {"
            + "    \"total_records_written\": 100,"
            + "    \"failures\": []"
            + "  }"
            + "}"
            + "}";

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);
    result.setStartTimeMs(System.currentTimeMillis() - 1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("RUN_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    // Should not throw and should still record the run
    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);

    // events_produced should be 0 (missingNode fallback, not silently reading source.type etc.)
    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertTrue(eventsCounter == null || eventsCounter.count() == 0.0);

    // records_written from sink should still work
    Counter recordsCounter = meterRegistry.find("com.datahub.ingest.records_written").counter();
    assertNotNull(recordsCounter);
    assertEquals(recordsCounter.count(), 100.0);
  }

  @Test
  public void testTablesScannedRecorded() throws Exception {
    String reportJson =
        "{"
            + "\"cli\": {\"cli_version\": \"0.14.0\"},"
            + "\"Source (snowflake)\": {"
            + "  \"events_produced\": 100,"
            + "  \"tables_scanned\": 20,"
            + "  \"warnings\": [],"
            + "  \"failures\": [],"
            + "  \"platform\": \"snowflake\""
            + "},"
            + "\"Sink (datahub-rest)\": {"
            + "  \"total_records_written\": 90,"
            + "  \"failures\": []"
            + "}"
            + "}";

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);
    result.setStartTimeMs(System.currentTimeMillis() - 1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    hook.invoke(event);

    DistributionSummary tablesScannedSummary =
        meterRegistry.find("com.datahub.ingest.tables_scanned").summary();
    assertNotNull(tablesScannedSummary);
    assertEquals(tablesScannedSummary.totalAmount(), 20.0);
  }

  @Test
  public void testViewParseFailuresRecorded() throws Exception {
    String reportJson =
        "{"
            + "\"cli\": {\"cli_version\": \"0.14.0\"},"
            + "\"Source (snowflake)\": {"
            + "  \"events_produced\": 100,"
            + "  \"num_view_definitions_failed_parsing\": 3,"
            + "  \"warnings\": [],"
            + "  \"failures\": [],"
            + "  \"platform\": \"snowflake\""
            + "},"
            + "\"Sink (datahub-rest)\": {"
            + "  \"total_records_written\": 90,"
            + "  \"failures\": []"
            + "}"
            + "}";

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);
    result.setStartTimeMs(System.currentTimeMillis() - 1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    hook.invoke(event);

    Counter viewParseFailuresCounter =
        meterRegistry.find("com.datahub.ingest.view_parse_failures").counter();
    assertNotNull(viewParseFailuresCounter);
    assertEquals(viewParseFailuresCounter.count(), 3.0);
  }

  @Test
  public void testRestateChangeTypeIgnored() throws Exception {
    // RESTATE is excluded intentionally: reindex operations replay historical MCLs as RESTATE,
    // which would double-count all ingestion metrics. Only UPSERT and CREATE are processed.
    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);
    event.setChangeType(ChangeType.RESTATE);

    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  // buildRunEventMap tests

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildRunEventMapHighStageSecondsEnumRemapping() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String reportJsonStr =
        "{"
            + "\"source\": {"
            + "  \"type\": \"snowflake\","
            + "  \"report\": {"
            + "    \"events_produced\": 100,"
            + "    \"ingestion_high_stage_seconds\": {\"_UNDEFINED\": 5.0, \"PROFILING\": 2.0, \"CUSTOM_STAGE\": 1.0},"
            + "    \"warnings\": [], \"failures\": []"
            + "  }"
            + "},"
            + "\"sink\": {"
            + "  \"type\": \"datahub-rest\","
            + "  \"report\": {\"total_records_written\": 90, \"failures\": []}"
            + "}"
            + "}";
    JsonNode reportJson = mapper.readTree(reportJsonStr);
    JsonNode sourceReport = reportJson.path("source").path("report");
    JsonNode sinkReport = reportJson.path("sink").path("report");

    IngestionMetricsHook.RunEvent runEvent =
        new IngestionMetricsHook.RunEvent(
            Urn.createFromString(TEST_EXECUTION_REQUEST_URN),
            "snowflake",
            "SUCCEEDED",
            1000L,
            100L,
            90L,
            0,
            0,
            0,
            reportJson,
            sourceReport,
            sinkReport);

    Map<String, Object> event = hook.buildRunEventMap(runEvent);

    Map<String, Object> highStage = (Map<String, Object>) event.get("ingestion_high_stage_seconds");
    assertNotNull(highStage);
    assertTrue(highStage.containsKey("Ingestion")); // _UNDEFINED → Ingestion
    assertTrue(highStage.containsKey("Profiling")); // PROFILING → Profiling
    assertTrue(highStage.containsKey("CUSTOM_STAGE")); // unknown keys pass through unchanged
    assertTrue(!highStage.containsKey("_UNDEFINED"));
    assertTrue(!highStage.containsKey("PROFILING"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildRunEventMapExtractLogEntriesStringAndObject() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    // Source failures are StructuredLogEntry objects; sink failures are plain strings.
    String reportJsonStr =
        "{"
            + "\"cli\": {\"cli_version\": \"0.14.0\"},"
            + "\"Source (snowflake)\": {"
            + "  \"events_produced\": 50,"
            + "  \"failures\": [{\"title\": \"Schema error\", \"message\": \"bad schema\","
            + "    \"context\": [\"Table: foo\"], \"log_category\": \"SCHEMA\"}],"
            + "  \"warnings\": [], \"infos\": [],"
            + "  \"platform\": \"snowflake\""
            + "},"
            + "\"Sink (datahub-rest)\": {"
            + "  \"total_records_written\": 45,"
            + "  \"failures\": [\"write timeout\", \"connection reset\"]"
            + "}"
            + "}";
    JsonNode reportJson = mapper.readTree(reportJsonStr);
    JsonNode sourceReport = reportJson.path("Source (snowflake)");
    JsonNode sinkReport = reportJson.path("Sink (datahub-rest)");

    IngestionMetricsHook.RunEvent runEvent =
        new IngestionMetricsHook.RunEvent(
            Urn.createFromString(TEST_EXECUTION_REQUEST_URN),
            "snowflake",
            "FAILURE",
            500L,
            50L,
            45L,
            0,
            1,
            2,
            reportJson,
            sourceReport,
            sinkReport);

    Map<String, Object> event = hook.buildRunEventMap(runEvent);

    // Source failures: StructuredLogEntry with title, message, context, category
    List<Map<String, Object>> failures = (List<Map<String, Object>>) event.get("failures");
    assertNotNull(failures);
    assertEquals(failures.size(), 1);
    assertEquals(failures.get(0).get("title"), "Schema error");
    assertEquals(failures.get(0).get("message"), "bad schema");
    assertEquals(failures.get(0).get("category"), "SCHEMA");
    List<String> context = (List<String>) failures.get(0).get("context");
    assertNotNull(context);
    assertEquals(context.get(0), "Table: foo");

    // Sink failures: plain strings serialized under "message"
    List<Map<String, Object>> sinkFailures = (List<Map<String, Object>>) event.get("sink_failures");
    assertNotNull(sinkFailures);
    assertEquals(sinkFailures.size(), 2);
    assertEquals(sinkFailures.get(0).get("message"), "write timeout");
    assertEquals(sinkFailures.get(1).get("message"), "connection reset");
  }

  // Helper methods

  private MetadataChangeLog createTestEvent(
      String status,
      Long durationMs,
      int eventsProduced,
      int recordsWritten,
      int warnings,
      int failures)
      throws Exception {

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus(status);
    if (durationMs != null) {
      result.setDurationMs(durationMs);
    }
    result.setStartTimeMs(System.currentTimeMillis() - (durationMs != null ? durationMs : 0));

    // Build structured report JSON using StructuredLogEntry format
    StringBuilder failuresJson = new StringBuilder("[");
    for (int i = 0; i < failures; i++) {
      if (i > 0) failuresJson.append(",");
      failuresJson
          .append("{\"title\":\"Extraction error\",\"message\":\"test error ")
          .append(i)
          .append("\",\"context\":[\"ErrorType: detail ")
          .append(i)
          .append("\"]}");
    }
    failuresJson.append("]");

    StringBuilder warningsJson = new StringBuilder("[");
    for (int i = 0; i < warnings; i++) {
      if (i > 0) warningsJson.append(",");
      warningsJson
          .append("{\"title\":\"Config issue\",\"message\":\"test warning ")
          .append(i)
          .append("\",\"context\":[]}");
    }
    warningsJson.append("]");

    String reportJson =
        String.format(
            "{"
                + "\"cli\": {\"cli_version\": \"0.14.0\"},"
                + "\"Source (file)\": {"
                + "  \"events_produced\": %d,"
                + "  \"warnings\": %s,"
                + "  \"failures\": %s,"
                + "  \"platform\": \"file\""
                + "},"
                + "\"Sink (datahub-rest)\": {"
                + "  \"total_records_written\": %d,"
                + "  \"failures\": []"
                + "}"
                + "}",
            eventsProduced, warningsJson, failuresJson, recordsWritten);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    return event;
  }

  /**
   * Creates a test event in executor format (RUN_INGEST). The executor uses a different JSON
   * structure: {"source": {"type": "...", "report": {...}}, "sink": {"type": "...", "report":
   * {...}}}
   */
  private MetadataChangeLog createExecutorTestEvent(
      String status,
      Long durationMs,
      int eventsProduced,
      int recordsWritten,
      int warnings,
      int failures)
      throws Exception {

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus(status);
    if (durationMs != null) {
      result.setDurationMs(durationMs);
    }
    result.setStartTimeMs(System.currentTimeMillis() - (durationMs != null ? durationMs : 0));

    // Build structured report JSON in executor format using StructuredLogEntry format
    StringBuilder failuresJson = new StringBuilder("[");
    for (int i = 0; i < failures; i++) {
      if (i > 0) failuresJson.append(",");
      failuresJson
          .append("{\"title\":\"Extraction error\",\"message\":\"test error ")
          .append(i)
          .append("\",\"context\":[\"ErrorType: detail ")
          .append(i)
          .append("\"]}");
    }
    failuresJson.append("]");

    StringBuilder warningsJson = new StringBuilder("[");
    for (int i = 0; i < warnings; i++) {
      if (i > 0) warningsJson.append(",");
      warningsJson
          .append("{\"title\":\"Config issue\",\"message\":\"test warning ")
          .append(i)
          .append("\",\"context\":[]}");
    }
    warningsJson.append("]");

    // Executor format: nested source.report and sink.report
    String reportJson =
        String.format(
            "{"
                + "\"cli\": {\"cli_version\": \"1.3.1\"},"
                + "\"source\": {"
                + "  \"type\": \"snowflake\","
                + "  \"report\": {"
                + "    \"events_produced\": %d,"
                + "    \"warnings\": %s,"
                + "    \"failures\": %s,"
                + "    \"platform\": \"snowflake\""
                + "  }"
                + "},"
                + "\"sink\": {"
                + "  \"type\": \"datahub-rest\","
                + "  \"report\": {"
                + "    \"total_records_written\": %d,"
                + "    \"failures\": []"
                + "  }"
                + "}"
                + "}",
            eventsProduced, warningsJson, failuresJson, recordsWritten);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("RUN_INGEST"); // Executor uses RUN_INGEST
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    return event;
  }
}
