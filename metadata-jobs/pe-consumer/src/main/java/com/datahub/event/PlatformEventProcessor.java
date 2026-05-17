package com.datahub.event;

import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.PlatformEvent;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(PlatformEventProcessorCondition.class)
public class PlatformEventProcessor {
  public static final String DATAHUB_PLATFORM_EVENT_CONSUMER_GROUP_VALUE =
      "${PLATFORM_EVENT_KAFKA_CONSUMER_GROUP_ID:generic-platform-event-job-client}";

  private final OperationContext systemOperationContext;

  @Getter private final List<PlatformEventHook> hooks;

  @Value(DATAHUB_PLATFORM_EVENT_CONSUMER_GROUP_VALUE)
  private String datahubPlatformEventConsumerGroupId;

  @Autowired
  public PlatformEventProcessor(
      @Qualifier("systemOperationContext") @Nonnull final OperationContext systemOperationContext,
      List<PlatformEventHook> platformEventHooks) {
    log.info("Creating Platform Event Processor");
    this.systemOperationContext = systemOperationContext;
    this.hooks =
        platformEventHooks.stream()
            .filter(PlatformEventHook::isEnabled)
            .collect(Collectors.toList());
    log.info(
        "Enabled platform hooks: {}",
        this.hooks.stream()
            .map(hook -> hook.getClass().getSimpleName())
            .collect(Collectors.toList()));
    this.hooks.forEach(PlatformEventHook::init);
  }

  /** Used by {@link PlatformEventKafkaListener}. */
  public void consumeKafka(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    systemOperationContext.withSpan(
        "consume",
        () ->
            processPlatformEvent(
                consumerRecord.value(),
                consumerRecord.key(),
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.serializedValueSize(),
                consumerRecord.timestamp()),
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }

  /** Used by pgQueue poller when {@code datahub.messaging.transport=pgqueue}. */
  public void consumePgQueue(InboundMetadataEnvelope<GenericRecord> envelope) {
    systemOperationContext.withSpan(
        "consume",
        () ->
            processPlatformEvent(
                envelope.getPayload(),
                envelope.getKey(),
                envelope.getLogicalTopic(),
                Objects.requireNonNullElse(envelope.getKafkaPartition(), 0),
                Objects.requireNonNullElse(envelope.getKafkaOffset(), 0L),
                Objects.requireNonNullElse(envelope.getSerializedValueSize(), 0),
                envelope.getEnqueuedAtMillis()),
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }

  private void processPlatformEvent(
      final GenericRecord record,
      final String key,
      final String topic,
      final int partition,
      final long offset,
      final int serializedValueSize,
      final long timestampMillis) {

    log.debug("Consuming a Platform Event");

    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metricUtils -> {
              long queueTimeMs = System.currentTimeMillis() - timestampMillis;

              // Dropwizard legacy
              metricUtils.histogram(this.getClass(), "kafkaLag", queueTimeMs);

              // Micrometer with tags
              // TODO: include priority level when available
              metricUtils
                  .getRegistry()
                  .timer(
                      MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
                      "topic",
                      topic,
                      "consumer.group",
                      datahubPlatformEventConsumerGroupId)
                  .record(Duration.ofMillis(queueTimeMs));
            });
    log.debug(
        "Got PE event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
        key,
        topic,
        partition,
        offset,
        serializedValueSize,
        timestampMillis);
    systemOperationContext
        .getMetricUtils()
        .ifPresent(metricUtils -> metricUtils.increment(this.getClass(), "received_pe_count", 1));

    PlatformEvent event;
    try {
      if (record == null) {
        log.error("Null record found. Dropping.");
        systemOperationContext
            .getMetricUtils()
            .ifPresent(metricUtils -> metricUtils.increment(this.getClass(), "null_record", 1));
        return;
      }

      event = EventUtils.avroToPegasusPE(record);
      log.debug("Successfully converted Avro PE to Pegasus PE. name: {}", event.getName());
    } catch (Exception e) {
      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(this.getClass(), "avro_to_pegasus_conversion_failure", 1));
      log.error("Error deserializing message due to: ", e);
      log.error("Message: {}", record.toString());
      return;
    }

    log.debug("Invoking PE hooks for event name {}", event.getName());

    for (PlatformEventHook hook : this.hooks) {
      log.debug(
          "Invoking PE hook {} for event name {}",
          hook.getClass().getSimpleName(),
          event.getName());

      systemOperationContext.withSpan(
          hook.getClass().getSimpleName(),
          () -> {
            try {
              hook.invoke(systemOperationContext, event);
            } catch (Exception e) {
              // Just skip this hook and continue.
              systemOperationContext
                  .getMetricUtils()
                  .ifPresent(
                      metricUtils ->
                          metricUtils.increment(
                              this.getClass(), hook.getClass().getSimpleName() + "_failure", 1));
              log.error(
                  "Failed to execute PE hook with name {}", hook.getClass().getCanonicalName(), e);
            }
          },
          MetricUtils.DROPWIZARD_NAME,
          MetricUtils.name(this.getClass(), hook.getClass().getSimpleName() + "_latency"));
    }
    systemOperationContext
        .getMetricUtils()
        .ifPresent(metricUtils -> metricUtils.increment(this.getClass(), "consumed_pe_count", 1));
    log.debug("Successfully completed PE hooks for event with name {}", event.getName());
  }
}
