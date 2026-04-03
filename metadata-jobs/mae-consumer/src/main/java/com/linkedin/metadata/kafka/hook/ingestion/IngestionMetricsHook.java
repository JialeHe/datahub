package com.linkedin.metadata.kafka.hook.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * MAE consumer hook that converts {@link ExecutionRequestResult} MCL events into Micrometer metrics
 * and structured log events for ingestion observability.
 *
 * <p>Design decisions:
 *
 * <ul>
 *   <li>Only UPSERT and CREATE change types are processed — RESTATE is excluded because reindex
 *       operations replay historical MCLs and would double-count all counters.
 *   <li>Metric labels use only stable, low-cardinality values (platform, status, customer). Per-run
 *       identifiers and high-cardinality fields (pipeline ID, version, ingestion_source,
 *       py_version, gms_version) are emitted in the {@code [INGESTION_RUN_EVENT]} structured log
 *       only.
 *   <li>warnings/failures counters reflect the JSON array size, which the ingestion framework caps
 *       at 10 via LossyList. A run with 200 failures shows failures_count=10. No total count field
 *       exists in the structured report.
 *   <li>No entity client RPCs are made — all data comes from the MCL event itself. The ingestion
 *       source can be looked up via GraphQL using the execution_id in the structured log.
 * </ul>
 */
@Slf4j
@Component
public class IngestionMetricsHook implements MetadataChangeLogHook {

  private static final String ASPECT_NAME = "dataHubExecutionRequestResult";
  // JMX-style prefix intentionally chosen: Micrometer converts dots to underscores in Prometheus,
  // producing com_datahub_ingest_* which matches the Observe Agent allowlist filter regex.
  private static final String METRIC_PREFIX = "com.datahub.ingest.";
  private static final Set<String> SUPPORTED_REPORT_TYPES =
      ImmutableSet.of("CLI_INGEST", "RUN_INGEST");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // RESTATE excluded: reindex replays MCLs as RESTATE, which would double-count all metrics.
  private static final Set<ChangeType> SUPPORTED_CHANGE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.CREATE);

  // 3 metric labels: platform, status, customer.
  // version, ingestion_source removed — unbounded cardinality. Both in [INGESTION_RUN_EVENT] log.
  private static final String TAG_PLATFORM = "platform";
  private static final String TAG_STATUS = "status";
  private static final String TAG_CUSTOMER = "customer";
  private final MeterRegistry meterRegistry;
  private final boolean enabled;
  @Getter private final String consumerGroupSuffix;
  private final String customerId;

  @Autowired
  public IngestionMetricsHook(
      @Nonnull final MeterRegistry meterRegistry,
      @Value("${ingestionMetrics.hook.enabled:false}") final boolean isEnabled,
      @Nonnull @Value("${ingestionMetrics.hook.consumerGroupSuffix:}")
          final String consumerGroupSuffix,
      @Nullable @Value("${executors.executorCustomerId:#{null}}") final String customerId) {
    this.meterRegistry = meterRegistry;
    this.enabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
    this.customerId = customerId != null ? customerId : "unknown";
  }

  @VisibleForTesting
  public IngestionMetricsHook(@Nonnull MeterRegistry meterRegistry, boolean isEnabled) {
    this(meterRegistry, isEnabled, "", null);
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public IngestionMetricsHook init(@Nonnull OperationContext systemOperationContext) {
    log.info("Initialized the ingestion metrics hook");
    return this;
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) throws Exception {
    if (!enabled || !isEligibleForProcessing(event)) {
      return;
    }

    try {
      ExecutionRequestResult result = extractResult(event);

      if (!isIngestionReport(result)) {
        return;
      }

      recordMetrics(result, event.getEntityUrn());
    } catch (Exception e) {
      log.error("Failed to process ingestion metrics for event: {}", event.getEntityUrn(), e);
    }
  }

  private boolean isEligibleForProcessing(@Nonnull MetadataChangeLog event) {
    return ASPECT_NAME.equals(event.getAspectName())
        && SUPPORTED_CHANGE_TYPES.contains(event.getChangeType())
        && event.getAspect() != null
        && event.getEntityUrn() != null;
  }

  private boolean isIngestionReport(@Nonnull ExecutionRequestResult result) {
    if (!result.hasStructuredReport()) {
      return false;
    }
    StructuredExecutionReport report = result.getStructuredReport();
    return SUPPORTED_REPORT_TYPES.contains(report.getType());
  }

  @Nonnull
  private ExecutionRequestResult extractResult(@Nonnull MetadataChangeLog event) {
    return GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        ExecutionRequestResult.class);
  }

  private void recordMetrics(
      @Nonnull ExecutionRequestResult result, @Nonnull Urn executionRequestUrn) {
    try {
      JsonNode reportJson = parseStructuredReport(result);
      if (reportJson == null) {
        log.warn("Could not parse structured report for {}", executionRequestUrn);
        return;
      }

      JsonNode sourceReport = findSourceReport(reportJson);
      JsonNode sinkReport = findSinkReport(reportJson);

      String platform = extractPlatform(reportJson, sourceReport);
      String status = result.getStatus() != null ? result.getStatus() : "unknown";

      Tags tags =
          Tags.of(
              TAG_PLATFORM, sanitizeTagValue(platform),
              TAG_STATUS, sanitizeTagValue(status),
              TAG_CUSTOMER, sanitizeTagValue(customerId));

      long eventsProduced = extractLongField(sourceReport, "events_produced");
      // NOTE: warnings and failures are counted by array size, but the ingestion framework's
      // LossyList caps these arrays at 10 entries. A run with 200 failures emits failures_count=10.
      // There is no separate total count field in the structured report — this is a known
      // limitation.
      // LossyList in the ingestion framework always serializes as a JSON array.
      int warningsCount = extractArraySize(sourceReport, "warnings");
      int failuresCount = extractArraySize(sourceReport, "failures");
      long recordsWritten = extractLongField(sinkReport, "total_records_written");
      int sinkFailuresCount = extractArraySize(sinkReport, "failures");

      Counter.builder(METRIC_PREFIX + "runs")
          .tags(tags)
          .description("Total number of ingestion runs")
          .register(meterRegistry)
          .increment();

      if (result.hasDurationMs()) {
        DistributionSummary.builder(METRIC_PREFIX + "duration_ms")
            .tags(tags)
            .description("Duration of ingestion runs in milliseconds")
            .register(meterRegistry)
            .record(result.getDurationMs());
      }

      // Register all counters unconditionally so Prometheus has the series even for zero-value
      // runs. Without this, rate() returns no data instead of 0 for label combinations that
      // haven't crossed the zero threshold, causing dashboard holes.
      Counter.builder(METRIC_PREFIX + "events_produced")
          .tags(tags)
          .description("Metadata records extracted from source")
          .register(meterRegistry)
          .increment(eventsProduced);
      Counter.builder(METRIC_PREFIX + "warnings")
          .tags(tags)
          .description("Source warning count per ingestion run")
          .register(meterRegistry)
          .increment(warningsCount);
      Counter.builder(METRIC_PREFIX + "failures")
          .tags(tags)
          .description("Source extraction error count per ingestion run")
          .register(meterRegistry)
          .increment(failuresCount);
      Counter.builder(METRIC_PREFIX + "records_written")
          .tags(tags)
          .description("Records successfully written to DataHub")
          .register(meterRegistry)
          .increment(recordsWritten);
      Counter.builder(METRIC_PREFIX + "sink_failures")
          .tags(tags)
          .description("Sink write failures — indicates data loss")
          .register(meterRegistry)
          .increment(sinkFailuresCount);

      long tablesScanned = extractLongField(sourceReport, "tables_scanned");
      DistributionSummary.builder(METRIC_PREFIX + "tables_scanned")
          .tags(tags)
          .description("Number of tables scanned per ingestion run")
          .register(meterRegistry)
          .record(tablesScanned);

      long viewParseFailures =
          extractLongField(sourceReport, "num_view_definitions_failed_parsing");
      Counter.builder(METRIC_PREFIX + "view_parse_failures")
          .tags(tags)
          .description(
              "View definitions that failed to parse — silent failures not counted in failures metric")
          .register(meterRegistry)
          .increment(viewParseFailures);

      // Per-execution detail as structured log (Observe ingests this for drill-down)
      emitRunEvent(
          new RunEvent(
              executionRequestUrn,
              platform,
              status,
              result.hasDurationMs() ? result.getDurationMs() : null,
              eventsProduced,
              recordsWritten,
              warningsCount,
              failuresCount,
              sinkFailuresCount,
              reportJson,
              sourceReport,
              sinkReport));

    } catch (Exception e) {
      log.error("Error recording metrics for {}: {}", executionRequestUrn, e.getMessage(), e);
    }
  }

  @Nullable
  private JsonNode parseStructuredReport(@Nonnull ExecutionRequestResult result) {
    try {
      if (!result.hasStructuredReport()) {
        return null;
      }
      String serializedValue = result.getStructuredReport().getSerializedValue();
      return OBJECT_MAPPER.readTree(serializedValue);
    } catch (Exception e) {
      log.warn("Failed to parse structured report JSON: {}", e.getMessage());
      return null;
    }
  }

  @Nonnull
  private String extractPlatform(@Nonnull JsonNode reportJson, @Nonnull JsonNode sourceReport) {
    // Try executor format first: source.type
    JsonNode sourceType = reportJson.path("source").path("type");
    if (!sourceType.isMissingNode() && sourceType.isTextual()) {
      return sourceType.asText();
    }

    // Fall back to platform field in source report (works for both formats)
    if (!sourceReport.isMissingNode()) {
      JsonNode platform = sourceReport.path("platform");
      if (!platform.isMissingNode() && platform.isTextual()) {
        return platform.asText();
      }
    }
    return "unknown";
  }

  /**
   * Finds the source report in the JSON (handles multiple formats).
   *
   * <p>Executor format (RUN_INGEST): {"source": {"type": "...", "report": {...}}}
   *
   * <p>CLI format: {"Source (type)": {...}} or keys starting with "Source ("
   */
  @Nonnull
  private JsonNode findSourceReport(@Nonnull JsonNode reportJson) {
    // Try executor format first: source.report
    JsonNode source = reportJson.path("source");
    if (!source.isMissingNode()) {
      JsonNode report = source.path("report");
      if (!report.isMissingNode()) {
        return report;
      }
      // source exists but has no nested report — return missingNode to avoid silently reading
      // fields from the source parent object (which would return 0 for all numeric lookups)
      return MissingNode.getInstance();
    }

    // Fall back to CLI format: "Source (type)" keys
    var fields = reportJson.fields();
    while (fields.hasNext()) {
      var entry = fields.next();
      if (entry.getKey().startsWith("Source (")) {
        return entry.getValue();
      }
    }
    return MissingNode.getInstance();
  }

  /**
   * Finds the sink report in the JSON (handles multiple formats).
   *
   * <p>Executor format (RUN_INGEST): {"sink": {"type": "...", "report": {...}}}
   *
   * <p>CLI format: {"Sink (type)": {...}} or keys starting with "Sink ("
   */
  @Nonnull
  private JsonNode findSinkReport(@Nonnull JsonNode reportJson) {
    // Try executor format first: sink.report
    JsonNode sink = reportJson.path("sink");
    if (!sink.isMissingNode()) {
      JsonNode report = sink.path("report");
      if (!report.isMissingNode()) {
        return report;
      }
      // sink exists but has no nested report — return missingNode to avoid silently reading
      // fields from the sink parent object
      return MissingNode.getInstance();
    }

    // Fall back to CLI format: "Sink (type)" keys
    var fields = reportJson.fields();
    while (fields.hasNext()) {
      var entry = fields.next();
      if (entry.getKey().startsWith("Sink (")) {
        return entry.getValue();
      }
    }
    return MissingNode.getInstance();
  }

  private long extractLongField(@Nonnull JsonNode parent, @Nonnull String fieldName) {
    JsonNode value = parent.path(fieldName);
    return (!value.isMissingNode() && value.isNumber()) ? value.asLong() : 0;
  }

  private int extractArraySize(@Nonnull JsonNode parent, @Nonnull String fieldName) {
    JsonNode array = parent.path(fieldName);
    return (!array.isMissingNode() && array.isArray()) ? array.size() : 0;
  }

  /** Per-execution detail passed to emitRunEvent to avoid long parameter lists. */
  record RunEvent(
      Urn executionRequestUrn,
      String platform,
      String status,
      Long durationMs,
      long eventsProduced,
      long recordsWritten,
      int warningsCount,
      int failuresCount,
      int sinkFailuresCount,
      JsonNode reportJson,
      JsonNode sourceReport,
      JsonNode sinkReport) {}

  private void emitRunEvent(@Nonnull RunEvent r) {
    if (!log.isInfoEnabled()) {
      return;
    }
    try {
      log.info("[INGESTION_RUN_EVENT] {}", OBJECT_MAPPER.writeValueAsString(buildRunEventMap(r)));
    } catch (Exception e) {
      log.warn("Failed to emit structured run event: {}", e.getMessage());
    }
  }

  @VisibleForTesting
  Map<String, Object> buildRunEventMap(@Nonnull RunEvent r) {
    Map<String, Object> event = new LinkedHashMap<>();

    event.put("execution_id", r.executionRequestUrn().getId());
    event.put("customer", customerId);
    event.put("platform", r.platform());
    event.put("status", r.status());
    if (r.durationMs() != null) {
      event.put("duration_ms", r.durationMs());
    }

    JsonNode cli = r.reportJson().path("cli");
    if (!cli.isMissingNode()) {
      putIfPresent(event, "cli_version", cli, "cli_version");
      putIfPresent(event, "models_version", cli, "models_version");
      putIfPresent(event, "py_version", cli, "py_version");
      putIfPresent(event, "mem_info", cli, "mem_info");
      putIfPresent(event, "peak_memory_usage", cli, "peak_memory_usage");
      putIfPresent(event, "peak_disk_usage", cli, "peak_disk_usage");
      if (!cli.path("thread_count").isMissingNode()) {
        event.put("thread_count", cli.path("thread_count").asLong());
      }
      if (!cli.path("peak_thread_count").isMissingNode()) {
        event.put("peak_thread_count", cli.path("peak_thread_count").asLong());
      }
    }

    event.put("events_produced", r.eventsProduced());
    putLongIfPresent(event, "tables_scanned", r.sourceReport(), "tables_scanned");
    putLongIfPresent(event, "views_scanned", r.sourceReport(), "views_scanned");
    putLongIfPresent(event, "schemas_scanned", r.sourceReport(), "schemas_scanned");
    putLongIfPresent(event, "databases_scanned", r.sourceReport(), "databases_scanned");
    putLongIfPresent(event, "entities_profiled", r.sourceReport(), "entities_profiled");
    putLongIfPresent(
        event,
        "num_view_definitions_failed_parsing",
        r.sourceReport(),
        "num_view_definitions_failed_parsing");

    if (!r.sourceReport().path("ingestion_stage_durations").isMissingNode()) {
      event.put("ingestion_stage_durations", r.sourceReport().path("ingestion_stage_durations"));
    }
    if (!r.sourceReport().path("ingestion_high_stage_seconds").isMissingNode()) {
      // Remap Python enum names to human-readable values.
      // Source: metadata-ingestion/src/datahub/ingestion/source_report/ingestion_stage.py
      // IngestionHighStage._UNDEFINED = "Ingestion", IngestionHighStage.PROFILING = "Profiling"
      // If the Python enum changes, these keys will silently become stale in Observe logs.
      Map<String, Object> highStageSeconds = new LinkedHashMap<>();
      r.sourceReport()
          .path("ingestion_high_stage_seconds")
          .fields()
          .forEachRemaining(
              entry -> {
                String key =
                    entry.getKey().equals("_UNDEFINED")
                        ? "Ingestion"
                        : entry.getKey().equals("PROFILING") ? "Profiling" : entry.getKey();
                highStageSeconds.put(key, entry.getValue().asDouble());
              });
      event.put("ingestion_high_stage_seconds", highStageSeconds);
    }

    event.put("warnings_count", r.warningsCount());
    event.put("failures_count", r.failuresCount());
    event.put("failures", extractLogEntries(r.sourceReport(), "failures"));
    event.put("warnings", extractLogEntries(r.sourceReport(), "warnings"));
    event.put("infos", extractLogEntries(r.sourceReport(), "infos"));

    event.put("records_written", r.recordsWritten());
    event.put("sink_failures_count", r.sinkFailuresCount());
    event.put("sink_failures", extractLogEntries(r.sinkReport(), "failures"));
    putDoubleIfPresent(
        event, "records_written_per_second", r.sinkReport(), "records_written_per_second");
    putLongIfPresent(event, "pending_requests", r.sinkReport(), "pending_requests");
    putIfPresent(event, "gms_version", r.sinkReport(), "gms_version");
    putIfPresent(event, "sink_mode", r.sinkReport(), "mode");

    return event;
  }

  /** Adds a string field to the event map if present and non-null in the JSON node. */
  private void putIfPresent(
      @Nonnull Map<String, Object> event,
      @Nonnull String key,
      @Nonnull JsonNode node,
      @Nonnull String field) {
    JsonNode value = node.path(field);
    if (!value.isMissingNode() && !value.isNull()) {
      event.put(key, value.asText());
    }
  }

  /** Adds a long field to the event map if present and numeric in the JSON node. */
  private void putLongIfPresent(
      @Nonnull Map<String, Object> event,
      @Nonnull String key,
      @Nonnull JsonNode node,
      @Nonnull String field) {
    JsonNode value = node.path(field);
    if (!value.isMissingNode() && value.isNumber()) {
      event.put(key, value.asLong());
    }
  }

  private void putDoubleIfPresent(
      @Nonnull Map<String, Object> event,
      @Nonnull String key,
      @Nonnull JsonNode node,
      @Nonnull String field) {
    JsonNode value = node.path(field);
    if (!value.isMissingNode() && value.isNumber()) {
      event.put(key, value.asDouble());
    }
  }

  /**
   * Extracts structured log entries (title, message, context, category) from a JSON array field.
   * Returns a list of maps for JSON serialization. The ingestion framework caps entries at 10 per
   * level via LossyDict, so no additional cap is needed here.
   */
  @Nonnull
  private List<Map<String, Object>> extractLogEntries(
      @Nonnull JsonNode report, @Nonnull String fieldName) {
    List<Map<String, Object>> entries = new ArrayList<>();
    JsonNode array = report.path(fieldName);
    if (array.isMissingNode() || !array.isArray()) {
      return entries;
    }
    for (JsonNode entry : array) {
      Map<String, Object> entryMap = new LinkedHashMap<>();
      if (entry.isTextual()) {
        // Sink failures are simple strings
        entryMap.put("message", entry.asText());
      } else if (entry.isObject()) {
        // Source failures/warnings are StructuredLogEntry objects
        if (entry.has("title") && !entry.get("title").isNull()) {
          entryMap.put("title", entry.get("title").asText());
        }
        if (entry.has("message")) {
          entryMap.put("message", entry.get("message").asText());
        }
        if (entry.has("context") && entry.get("context").isArray()) {
          List<String> context = new ArrayList<>();
          for (JsonNode c : entry.get("context")) {
            context.add(c.asText());
          }
          entryMap.put("context", context);
        }
        if (entry.has("log_category") && !entry.get("log_category").isNull()) {
          entryMap.put("category", entry.get("log_category").asText());
        }
      }
      if (!entryMap.isEmpty()) {
        entries.add(entryMap);
      }
    }
    return entries;
  }

  /**
   * Sanitizes tag values for Prometheus. Note: different inputs may map to the same output (e.g.,
   * "my:platform" and "my_platform" both become "my_platform"). This is acceptable since tag values
   * in practice (platform names, customer IDs) do not contain special characters.
   */
  @Nonnull
  private String sanitizeTagValue(@Nullable String value) {
    if (value == null || value.isEmpty()) {
      return "unknown";
    }
    return value.replaceAll("[^a-zA-Z0-9_.-]", "_");
  }

  @VisibleForTesting
  MeterRegistry getMeterRegistry() {
    return meterRegistry;
  }
}
