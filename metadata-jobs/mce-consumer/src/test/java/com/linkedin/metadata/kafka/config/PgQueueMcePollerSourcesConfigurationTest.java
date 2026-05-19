package com.linkedin.metadata.kafka.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.kafka.CDCProcessor;
import com.linkedin.metadata.kafka.MetadataChangeEventsProcessor;
import com.linkedin.metadata.kafka.MetadataChangeProposalConsumer;
import com.linkedin.metadata.kafka.batch.BatchMetadataChangeProposalsProcessor;
import com.linkedin.metadata.pgqueue.PgQueuePollContext;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.Test;

public class PgQueueMcePollerSourcesConfigurationTest {

  private final PgQueueMcePollerSourcesConfiguration configuration =
      new PgQueueMcePollerSourcesConfiguration();

  @Test
  public void pgQueueMcpSource_registersMcpPoller() {
    MetadataChangeProposalConsumer consumer = mock(MetadataChangeProposalConsumer.class);

    PgQueuePollerSource source =
        configuration.pgQueueMcpSource(consumer, "mce-group", Topics.METADATA_CHANGE_PROPOSAL);

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "mce-group");
    assertEquals(reg.topicNames(), List.of(Topics.METADATA_CHANGE_PROPOSAL));
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueueMceSource_registersMcePoller() {
    MetadataChangeEventsProcessor processor = mock(MetadataChangeEventsProcessor.class);

    PgQueuePollerSource source =
        configuration.pgQueueMceSource(processor, "mce-client", Topics.METADATA_CHANGE_EVENT);

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "mce-client");
    assertEquals(reg.topicNames(), List.of(Topics.METADATA_CHANGE_EVENT));
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueueCdcSource_registersCdcPoller() {
    CDCProcessor processor = mock(CDCProcessor.class);

    PgQueuePollerSource source =
        configuration.pgQueueCdcSource(processor, "cdc-topic", "cdc-group");

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "cdc-group");
    assertEquals(reg.topicNames(), List.of("cdc-topic"));
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueueMcpSource_handlerAcceptsAndCommits() throws Exception {
    MetadataChangeProposalConsumer consumer = mock(MetadataChangeProposalConsumer.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    GenericRecord record = mock(GenericRecord.class);
    when(deserializer.deserialize(anyString(), any(byte[].class))).thenReturn(record);

    PgQueuePollerRegistration reg =
        configuration
            .pgQueueMcpSource(consumer, "mce-group", Topics.METADATA_CHANGE_PROPOSAL)
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueReceivedMessage msg = sampleMessage();
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "mce-group", Duration.ofSeconds(30), deserializer);
    reg.handler().handleBatch(Topics.METADATA_CHANGE_PROPOSAL, List.of(msg), ctx);

    verify(consumer).accept(any(), eq("mce-group"));
    verify(store).commitForGroup(eq("mce-group"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void pgQueueMceSource_handlerConsumesAndCommits() throws Exception {
    MetadataChangeEventsProcessor processor = mock(MetadataChangeEventsProcessor.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    when(deserializer.deserialize(anyString(), any(byte[].class)))
        .thenReturn(mock(GenericRecord.class));

    PgQueuePollerRegistration reg =
        configuration
            .pgQueueMceSource(processor, "mce-client", Topics.METADATA_CHANGE_EVENT)
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueReceivedMessage msg = sampleMessage();
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "mce-client", Duration.ofSeconds(30), deserializer);
    reg.handler().handleBatch(Topics.METADATA_CHANGE_EVENT, List.of(msg), ctx);

    verify(processor).consumePgQueue(any());
    verify(store).commitForGroup(eq("mce-client"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void pgQueueCdcSource_handlerConsumesUtf8Payload() throws Exception {
    CDCProcessor processor = mock(CDCProcessor.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);

    PgQueuePollerRegistration reg =
        configuration
            .pgQueueCdcSource(processor, "cdc-topic", "cdc-group")
            .registrations()
            .findFirst()
            .orElseThrow();

    byte[] json = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
    QueueReceivedMessage msg = sampleMessage(json);
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "cdc-group", Duration.ofSeconds(30), null);
    reg.handler().handleBatch("cdc-topic", List.of(msg), ctx);

    verify(processor)
        .consumePgQueue(
            eq("cdc-topic"), eq("key"), eq("{\"a\":1}"), eq(0), eq(1L), eq(json.length), eq(0L));
    verify(store).commitForGroup(eq("cdc-group"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void pgQueueBatchMcpSource_handlerBuildsSyntheticRecords() throws Exception {
    BatchMetadataChangeProposalsProcessor batchProcessor =
        mock(BatchMetadataChangeProposalsProcessor.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    when(deserializer.deserialize(anyString(), any(byte[].class)))
        .thenReturn(mock(GenericRecord.class));

    PgQueuePollerRegistration reg =
        configuration
            .pgQueueBatchMcpSource(batchProcessor, "batch-group", Topics.METADATA_CHANGE_PROPOSAL)
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueReceivedMessage msg = sampleMessage();
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "batch-group", Duration.ofSeconds(30), deserializer);
    reg.handler().handleBatch(Topics.METADATA_CHANGE_PROPOSAL, List.of(msg), ctx);

    verify(batchProcessor).consume(any());
    verify(store).commitForGroup(eq("batch-group"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void pgQueueBatchMcpSource_registersBatchPoller() {
    BatchMetadataChangeProposalsProcessor batchProcessor =
        mock(BatchMetadataChangeProposalsProcessor.class);

    PgQueuePollerSource source =
        configuration.pgQueueBatchMcpSource(
            batchProcessor, "batch-group", Topics.METADATA_CHANGE_PROPOSAL);

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "batch-group");
    assertEquals(reg.threadName(), "pgqueue-batch-mcp-batch-group");
    assertNotNull(reg.handler());
  }

  private static QueueReceivedMessage sampleMessage() {
    return sampleMessage(new byte[] {1, 2, 3});
  }

  private static QueueReceivedMessage sampleMessage(byte[] payload) {
    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.EPOCH, 0, 0, 1L);
    return new QueueReceivedMessage(
        handle,
        0,
        payload,
        Optional.empty(),
        PgQueuePayloadCompression.NONE,
        List.of(),
        "key",
        "owner");
  }
}
