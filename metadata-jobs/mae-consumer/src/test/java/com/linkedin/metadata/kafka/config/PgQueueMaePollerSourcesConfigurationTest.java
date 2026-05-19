package com.linkedin.metadata.kafka.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.config.MetadataChangeLogConfig;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.kafka.DataHubUsageEventsProcessor;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.pgqueue.PgQueueBatchPolicy;
import com.linkedin.metadata.pgqueue.PgQueuePollContext;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.Test;

public class PgQueueMaePollerSourcesConfigurationTest {

  private final PgQueueMaePollerSourcesConfiguration configuration =
      new PgQueueMaePollerSourcesConfiguration();

  @Test
  public void pgQueueUsageSource_registersSinglePoller() {
    DataHubUsageEventsProcessor processor = mock(DataHubUsageEventsProcessor.class);

    PgQueuePollerSource source =
        configuration.pgQueueUsageSource(processor, "usage-group", Topics.DATAHUB_USAGE_EVENT);

    List<PgQueuePollerRegistration> registrations =
        source.registrations().collect(Collectors.toList());

    assertEquals(registrations.size(), 1);
    PgQueuePollerRegistration reg = registrations.get(0);
    assertEquals(reg.consumerGroupId(), "usage-group");
    assertEquals(reg.topicNames(), List.of(Topics.DATAHUB_USAGE_EVENT));
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueueMclHookSources_nonBatchMode() {
    MetadataChangeLogHook hook = mock(MetadataChangeLogHook.class);
    when(hook.isEnabled()).thenReturn(true);
    when(hook.getConsumerGroupSuffix()).thenReturn("hooks");
    when(hook.executionOrder()).thenReturn(1);

    ConfigurationProvider configurationProvider = mclConfigurationProvider(false, null);
    OperationContext operationContext = mock(OperationContext.class);

    PgQueuePollerSource source =
        configuration.pgQueueMclHookSources(
            List.of(hook),
            configurationProvider,
            new ObjectMapper(),
            operationContext,
            "mae-group",
            Topics.METADATA_CHANGE_LOG_VERSIONED,
            Topics.METADATA_CHANGE_LOG_TIMESERIES);

    List<PgQueuePollerRegistration> registrations =
        source.registrations().collect(Collectors.toList());

    assertEquals(registrations.size(), 1);
    PgQueuePollerRegistration reg = registrations.get(0);
    assertEquals(reg.consumerGroupId(), "mae-group-hooks");
    assertEquals(
        reg.topicNames(),
        List.of(Topics.METADATA_CHANGE_LOG_VERSIONED, Topics.METADATA_CHANGE_LOG_TIMESERIES));
    assertEquals(reg.batchPolicy(), null);
    assertEquals(reg.flushHandler(), null);
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueueMclHookSources_batchModeUsesAccumulationPolicy() {
    MetadataChangeLogHook hook = mock(MetadataChangeLogHook.class);
    when(hook.isEnabled()).thenReturn(true);
    when(hook.getConsumerGroupSuffix()).thenReturn("");
    when(hook.executionOrder()).thenReturn(0);

    MetadataChangeLogConfig.BatchConfig batch =
        MetadataChangeLogConfig.BatchConfig.builder()
            .enabled(true)
            .maxMessages(25)
            .size(2048)
            .maxAgeMs(250L)
            .build();
    ConfigurationProvider configurationProvider = mclConfigurationProvider(true, batch);

    PgQueuePollerSource source =
        configuration.pgQueueMclHookSources(
            List.of(hook),
            configurationProvider,
            new ObjectMapper(),
            mock(OperationContext.class),
            "generic-mae",
            "mcl-versioned",
            "mcl-timeseries");

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "generic-mae");
    PgQueueBatchPolicy policy = reg.batchPolicy();
    assertNotNull(policy);
    assertEquals(policy.maxMessages(), 25);
    assertEquals(policy.maxBytes(), 2048L);
    assertEquals(policy.maxAgeMs(), 250L);
    assertNotNull(reg.flushHandler());
  }

  @Test
  public void pgQueueMclHookSources_batchFlushHandlerInvokesHooks() throws Exception {
    MetadataChangeLogHook hook = mock(MetadataChangeLogHook.class);
    when(hook.isEnabled()).thenReturn(true);
    when(hook.getConsumerGroupSuffix()).thenReturn("");
    when(hook.executionOrder()).thenReturn(0);

    MetadataChangeLogConfig.BatchConfig batch =
        MetadataChangeLogConfig.BatchConfig.builder().enabled(true).build();
    ConfigurationProvider configurationProvider = mclConfigurationProvider(true, batch);

    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    GenericRecord record = mock(GenericRecord.class);
    when(deserializer.deserialize(anyString(), any(byte[].class))).thenReturn(record);

    PgQueuePollerRegistration reg =
        configuration
            .pgQueueMclHookSources(
                List.of(hook),
                configurationProvider,
                new ObjectMapper(),
                mock(OperationContext.class),
                "mae-batch",
                Topics.METADATA_CHANGE_LOG_VERSIONED,
                Topics.METADATA_CHANGE_LOG_TIMESERIES)
            .registrations()
            .findFirst()
            .orElseThrow();

    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    QueueReceivedMessage msg = sampleMessage();
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "mae-batch", Duration.ofSeconds(30), deserializer);

    try (var eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(mcl);
      reg.flushHandler().flush(Topics.METADATA_CHANGE_LOG_VERSIONED, List.of(msg), ctx);
    }

    verify(hook).invokeBatch(any());
    verify(store).commitForGroup(eq("mae-batch"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void isMclBatchEnabled_falseWhenConfigMissing() {
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.getMetadataChangeLog()).thenReturn(null);

    assertFalse(PgQueueMaePollerSourcesConfiguration.isMclBatchEnabled(provider));
  }

  @Test
  public void pgQueueUsageSource_handlerInvokesProcessorAndCommits() throws Exception {
    DataHubUsageEventsProcessor processor = mock(DataHubUsageEventsProcessor.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PgQueuePollerRegistration reg =
        configuration
            .pgQueueUsageSource(processor, "usage-group", Topics.DATAHUB_USAGE_EVENT)
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueReceivedMessage msg = sampleMessage();
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "usage-group", Duration.ofSeconds(30), null);
    reg.handler().handleBatch(Topics.DATAHUB_USAGE_EVENT, List.of(msg), ctx);

    verify(processor).consumePgQueue(eq(Topics.DATAHUB_USAGE_EVENT), eq(List.of(msg)));
    verify(store).commitForGroup(eq("usage-group"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void pgQueueMclHookSources_handlerDecodesAndAccepts() throws Exception {
    MetadataChangeLogHook hook = mock(MetadataChangeLogHook.class);
    when(hook.isEnabled()).thenReturn(true);
    when(hook.getConsumerGroupSuffix()).thenReturn("");
    when(hook.executionOrder()).thenReturn(0);

    ConfigurationProvider configurationProvider = mclConfigurationProvider(false, null);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    GenericRecord record = mock(GenericRecord.class);
    when(deserializer.deserialize(anyString(), any(byte[].class))).thenReturn(record);

    PgQueuePollerRegistration reg =
        configuration
            .pgQueueMclHookSources(
                List.of(hook),
                configurationProvider,
                new ObjectMapper(),
                mock(OperationContext.class),
                "mae-group",
                Topics.METADATA_CHANGE_LOG_VERSIONED,
                Topics.METADATA_CHANGE_LOG_TIMESERIES)
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueReceivedMessage msg = sampleMessage();
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "mae-group", Duration.ofSeconds(30), deserializer);
    reg.handler().handleBatch(Topics.METADATA_CHANGE_LOG_VERSIONED, List.of(msg), ctx);

    verify(store).commitForGroup(eq("mae-group"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void isMclBatchEnabled_trueWhenBatchEnabled() {
    MetadataChangeLogConfig.BatchConfig batch =
        MetadataChangeLogConfig.BatchConfig.builder().enabled(true).build();
    ConfigurationProvider provider = mclConfigurationProvider(true, batch);

    assertTrue(PgQueueMaePollerSourcesConfiguration.isMclBatchEnabled(provider));
  }

  private static ConfigurationProvider mclConfigurationProvider(
      boolean batchEnabled, MetadataChangeLogConfig.BatchConfig batch) {
    MetadataChangeLogConfig mcl = mock(MetadataChangeLogConfig.class);
    MetadataChangeLogConfig.ConsumerBatchConfig consumerBatch =
        mock(MetadataChangeLogConfig.ConsumerBatchConfig.class);
    MetadataChangeLogConfig.BatchConfig batchConfig =
        batch != null ? batch : mock(MetadataChangeLogConfig.BatchConfig.class);
    when(mcl.getConsumer()).thenReturn(consumerBatch);
    when(consumerBatch.getBatch()).thenReturn(batchConfig);
    if (batch == null) {
      when(batchConfig.isEnabled()).thenReturn(batchEnabled);
    }

    KafkaConfiguration kafka = mock(KafkaConfiguration.class);
    ConsumerConfiguration consumer = mock(ConsumerConfiguration.class);
    ConsumerConfiguration.ConsumerOptions mclConsumer =
        mock(ConsumerConfiguration.ConsumerOptions.class);
    when(kafka.getConsumer()).thenReturn(consumer);
    when(consumer.getMcl()).thenReturn(mclConsumer);
    when(mclConsumer.isFineGrainedLoggingEnabled()).thenReturn(false);
    when(mclConsumer.getAspectsToDrop()).thenReturn("");

    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.getMetadataChangeLog()).thenReturn(mcl);
    when(provider.getKafka()).thenReturn(kafka);
    return provider;
  }

  private static QueueReceivedMessage sampleMessage() {
    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.EPOCH, 0, 0, 1L);
    return new QueueReceivedMessage(
        handle,
        0,
        new byte[] {1, 2, 3},
        Optional.empty(),
        PgQueuePayloadCompression.NONE,
        List.of(),
        "key",
        "owner");
  }
}
