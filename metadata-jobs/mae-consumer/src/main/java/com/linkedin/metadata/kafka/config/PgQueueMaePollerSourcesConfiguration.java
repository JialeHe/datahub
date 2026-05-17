package com.linkedin.metadata.kafka.config;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.config.MetadataChangeLogConfig;
import com.linkedin.metadata.kafka.DataHubUsageEventsProcessor;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.kafka.listener.GenericKafkaListener;
import com.linkedin.metadata.kafka.listener.mcl.MCLKafkaListener;
import com.linkedin.metadata.pgqueue.PgQueueBatchPolicy;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/** pgQueue parallel supervisors for MAE (usage + MCL hook groups). */
@Slf4j
@Configuration
public class PgQueueMaePollerSourcesConfiguration {

  private static final int MCL_BATCH = 50;
  private static final int USAGE_BATCH = 200;

  private static final int DEFAULT_MCL_MAX_MESSAGES = 50;
  private static final int DEFAULT_MCL_MAX_BYTES = 1_048_576;
  private static final long DEFAULT_MCL_MAX_AGE_MS = 500;

  @Bean
  @Conditional(PgQueueMessagingAndDataHubUsageEventsProcessorCondition.class)
  public PgQueuePollerSource pgQueueUsageSource(
      DataHubUsageEventsProcessor processor,
      @Value(DataHubUsageEventsProcessor.DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_VALUE)
          String groupId,
      @Value("${DATAHUB_USAGE_EVENT_NAME:" + Topics.DATAHUB_USAGE_EVENT + "}") String topic) {
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                USAGE_BATCH,
                "pgqueue-usage-" + groupId,
                100,
                500,
                1000,
                (logicalTopic, batch, ctx) -> {
                  try {
                    processor.consumePgQueue(logicalTopic, batch);
                    ctx.commit(batch.stream().map(QueueReceivedMessage::handle).toList());
                  } catch (Exception e) {
                    log.error("Usage pgQueue batch failed; leases expire for retry", e);
                  }
                }));
  }

  @Bean
  @Conditional(PgQueueMessagingAndMetadataChangeLogProcessorCondition.class)
  public PgQueuePollerSource pgQueueMclHookSources(
      List<MetadataChangeLogHook> hooks,
      ConfigurationProvider configurationProvider,
      ObjectMapper objectMapper,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
          String consumerGroupBase,
      @Value(
              "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:"
                  + Topics.METADATA_CHANGE_LOG_VERSIONED
                  + "}")
          String mclVersionedTopicName,
      @Value(
              "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:"
                  + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}")
          String mclTimeseriesTopicName) {
    boolean batchEnabled = isMclBatchEnabled(configurationProvider);
    Map<String, Set<String>> aspectsToDrop =
        parseAspectsToDrop(configurationProvider, objectMapper);

    Map<String, List<MetadataChangeLogHook>> hookGroups =
        hooks.stream()
            .filter(MetadataChangeLogHook::isEnabled)
            .sorted(Comparator.comparing(MetadataChangeLogHook::executionOrder))
            .collect(Collectors.groupingBy(MetadataChangeLogHook::getConsumerGroupSuffix));

    log.info(
        "PgQueue MCL hook consumer groups (batch={}): {}",
        batchEnabled,
        hookGroups.keySet().stream()
            .map(s -> buildConsumerGroupName(consumerGroupBase, s))
            .collect(Collectors.toSet()));

    List<PgQueuePollerRegistration> registrations;
    if (batchEnabled) {
      PgQueueBatchPolicy policy = buildBatchPolicy(configurationProvider);
      registrations =
          hookGroups.entrySet().stream()
              .map(
                  e ->
                      buildBatchRegistration(
                          configurationProvider,
                          objectMapper,
                          systemOperationContext,
                          consumerGroupBase,
                          mclVersionedTopicName,
                          mclTimeseriesTopicName,
                          e.getKey(),
                          e.getValue(),
                          aspectsToDrop,
                          policy))
              .toList();
    } else {
      registrations =
          hookGroups.entrySet().stream()
              .map(
                  e -> {
                    final String gid = buildConsumerGroupName(consumerGroupBase, e.getKey());
                    final GenericKafkaListener<
                            MetadataChangeLog, MetadataChangeLogHook, GenericRecord>
                        listener =
                            createListener(
                                systemOperationContext,
                                configurationProvider,
                                objectMapper,
                                gid,
                                e.getValue());
                    return new PgQueuePollerRegistration(
                        gid,
                        List.of(mclVersionedTopicName, mclTimeseriesTopicName),
                        MCL_BATCH,
                        "pgqueue-mcl-" + gid,
                        25,
                        500,
                        1000,
                        (logicalTopic, batch, ctx) -> {
                          for (QueueReceivedMessage msg : batch) {
                            try {
                              GenericRecord record = ctx.decodeAvro(msg, logicalTopic);
                              InboundMetadataEnvelope<GenericRecord> envelope =
                                  InboundMetadataEnvelope.fromPgQueue(
                                      msg, logicalTopic, gid, record);
                              listener.acceptInbound(envelope);
                              ctx.commit(List.of(msg.handle()));
                            } catch (Exception ex) {
                              log.error(
                                  "MCL pgQueue processing failed for group {} topic {};"
                                      + " lease will expire for retry",
                                  gid,
                                  logicalTopic,
                                  ex);
                            }
                          }
                        });
                  })
              .toList();
    }

    return () -> registrations.stream();
  }

  private static PgQueuePollerRegistration buildBatchRegistration(
      ConfigurationProvider configurationProvider,
      ObjectMapper objectMapper,
      OperationContext systemOperationContext,
      String consumerGroupBase,
      String mclVersionedTopicName,
      String mclTimeseriesTopicName,
      String suffix,
      List<MetadataChangeLogHook> groupHooks,
      Map<String, Set<String>> aspectsToDrop,
      PgQueueBatchPolicy policy) {
    final String gid = buildConsumerGroupName(consumerGroupBase, suffix);

    // Initialise hooks via a throwaway listener (reuses init + executionOrder wiring)
    GenericKafkaListener<MetadataChangeLog, MetadataChangeLogHook, GenericRecord> listener =
        createListener(
            systemOperationContext, configurationProvider, objectMapper, gid, groupHooks);
    List<MetadataChangeLogHook> initHooks = listener.getHooks();

    return new PgQueuePollerRegistration(
        gid,
        List.of(mclVersionedTopicName, mclTimeseriesTopicName),
        MCL_BATCH,
        "pgqueue-mcl-batch-" + gid,
        25,
        500,
        1000,
        // per-poll handler is unused in accumulation mode but required by the record
        (logicalTopic, batch, ctx) -> {},
        policy,
        (logicalTopic, batch, ctx) -> {
          List<MetadataChangeLog> allMCLs = new ArrayList<>(batch.size());
          List<QueueMessageHandle> handles = new ArrayList<>(batch.size());
          for (QueueReceivedMessage msg : batch) {
            try {
              GenericRecord record = ctx.decodeAvro(msg, logicalTopic);
              MetadataChangeLog mcl = EventUtils.avroToPegasusMCL(record);
              if (!MCLKafkaListener.shouldSkipMcl(mcl, aspectsToDrop)) {
                allMCLs.add(mcl);
              }
              handles.add(msg.handle());
            } catch (Exception ex) {
              log.error("MCL pgQueue batch deserialization failed for group {}", gid, ex);
            }
          }
          if (!allMCLs.isEmpty()) {
            for (MetadataChangeLogHook hook : initHooks) {
              try {
                hook.invokeBatch(allMCLs);
              } catch (Exception ex) {
                log.error(
                    "MCL pgQueue batch hook {} failed for group {}",
                    hook.getClass().getSimpleName(),
                    gid,
                    ex);
              }
            }
          }
          ctx.commit(handles);
        });
  }

  private static PgQueueBatchPolicy buildBatchPolicy(ConfigurationProvider configurationProvider) {
    int maxMessages = DEFAULT_MCL_MAX_MESSAGES;
    long maxBytes = DEFAULT_MCL_MAX_BYTES;
    long maxAgeMs = DEFAULT_MCL_MAX_AGE_MS;
    try {
      MetadataChangeLogConfig.BatchConfig batch =
          configurationProvider.getMetadataChangeLog().getConsumer().getBatch();
      if (batch.getMaxMessages() != null) {
        maxMessages = batch.getMaxMessages();
      }
      if (batch.getSize() != null) {
        maxBytes = batch.getSize();
      }
      if (batch.getMaxAgeMs() != null) {
        maxAgeMs = batch.getMaxAgeMs();
      }
    } catch (Exception e) {
      log.warn("Unable to read MCL batch config, using defaults", e);
    }
    return new PgQueueBatchPolicy(maxMessages, maxBytes, maxAgeMs);
  }

  private static String buildConsumerGroupName(String consumerGroupBase, String suffix) {
    if (suffix.isEmpty()) {
      return consumerGroupBase;
    }
    return String.join("-", consumerGroupBase, suffix);
  }

  private static GenericKafkaListener<MetadataChangeLog, MetadataChangeLogHook, GenericRecord>
      createListener(
          OperationContext systemOperationContext,
          ConfigurationProvider configurationProvider,
          ObjectMapper objectMapper,
          String consumerGroupId,
          List<MetadataChangeLogHook> groupHooks) {
    boolean fineGrained =
        configurationProvider.getKafka().getConsumer().getMcl().isFineGrainedLoggingEnabled();
    Map<String, Set<String>> aspectsToDrop =
        parseAspectsToDrop(configurationProvider, objectMapper);
    MCLKafkaListener listener = new MCLKafkaListener();
    return listener.init(
        systemOperationContext, consumerGroupId, groupHooks, fineGrained, aspectsToDrop);
  }

  private static Map<String, Set<String>> parseAspectsToDrop(
      ConfigurationProvider configurationProvider, ObjectMapper objectMapper) {
    String aspectsToDropConfig =
        configurationProvider.getKafka().getConsumer().getMcl().getAspectsToDrop();
    if (StringUtils.isBlank(aspectsToDropConfig)) {
      return Collections.emptyMap();
    }
    JavaType type =
        objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Set.class);
    try {
      return objectMapper.readValue(aspectsToDropConfig, type);
    } catch (Exception e) {
      log.error("Unable to parse aspects to drop configuration: {}", aspectsToDropConfig, e);
      return Collections.emptyMap();
    }
  }

  static boolean isMclBatchEnabled(ConfigurationProvider configurationProvider) {
    try {
      if (configurationProvider.getMetadataChangeLog() != null
          && configurationProvider.getMetadataChangeLog().getConsumer() != null
          && configurationProvider.getMetadataChangeLog().getConsumer().getBatch() != null) {
        return configurationProvider.getMetadataChangeLog().getConsumer().getBatch().isEnabled();
      }
    } catch (Exception e) {
      log.debug("Error checking MCL batch configuration", e);
    }
    return false;
  }
}
