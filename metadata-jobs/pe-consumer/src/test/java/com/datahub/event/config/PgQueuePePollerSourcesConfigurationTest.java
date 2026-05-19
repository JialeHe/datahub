package com.datahub.event.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.datahub.event.PlatformEventProcessor;
import com.linkedin.metadata.pgqueue.PgQueuePollContext;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.Topics;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.Test;

public class PgQueuePePollerSourcesConfigurationTest {

  private final PgQueuePePollerSourcesConfiguration configuration =
      new PgQueuePePollerSourcesConfiguration();

  @Test
  public void pgQueuePeSource_registersPoller() {
    PlatformEventProcessor processor = mock(PlatformEventProcessor.class);

    PgQueuePollerSource source =
        configuration.pgQueuePeSource(processor, Topics.PLATFORM_EVENT, "pe-consumer-group");

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "pe-consumer-group");
    assertEquals(reg.topicNames(), List.of(Topics.PLATFORM_EVENT));
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueuePeSource_handlerConsumesAndCommits() throws Exception {
    PlatformEventProcessor processor = mock(PlatformEventProcessor.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    when(deserializer.deserialize(anyString(), any(byte[].class)))
        .thenReturn(mock(GenericRecord.class));

    PgQueuePollerRegistration reg =
        configuration
            .pgQueuePeSource(processor, "PlatformEvent_v1", "pe-group")
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.EPOCH, 0, 0, 1L);
    QueueReceivedMessage msg =
        new QueueReceivedMessage(
            handle,
            0,
            new byte[] {1},
            Optional.empty(),
            PgQueuePayloadCompression.NONE,
            List.of(),
            "key",
            "owner");
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "pe-group", Duration.ofSeconds(30), deserializer);
    reg.handler().handleBatch("PlatformEvent_v1", List.of(msg), ctx);

    verify(processor).consumePgQueue(any());
    verify(store).commitForGroup(eq("pe-group"), eq(List.of(handle)), eq(true));
  }
}
