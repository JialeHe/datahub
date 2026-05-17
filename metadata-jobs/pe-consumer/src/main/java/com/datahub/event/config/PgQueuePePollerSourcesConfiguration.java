package com.datahub.event.config;

import com.datahub.event.PlatformEventProcessor;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.Topics;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class PgQueuePePollerSourcesConfiguration {

  private static final int PE_BATCH = 50;

  @Bean
  @Conditional(PgQueueMessagingAndPlatformEventProcessorCondition.class)
  public PgQueuePollerSource pgQueuePeSource(
      PlatformEventProcessor processor,
      @Value("${PLATFORM_EVENT_TOPIC_NAME:" + Topics.PLATFORM_EVENT + "}") String topic,
      @Value(PlatformEventProcessor.DATAHUB_PLATFORM_EVENT_CONSUMER_GROUP_VALUE) String groupId) {
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                PE_BATCH,
                "pgqueue-pe-" + groupId,
                100,
                500,
                1000,
                (logicalTopic, batch, ctx) -> {
                  for (QueueReceivedMessage msg : batch) {
                    try {
                      GenericRecord record = ctx.decodeAvro(msg, logicalTopic);
                      InboundMetadataEnvelope<GenericRecord> envelope =
                          InboundMetadataEnvelope.fromPgQueue(msg, logicalTopic, groupId, record);
                      processor.consumePgQueue(envelope);
                      ctx.commit(List.of(msg.handle()));
                    } catch (Exception e) {
                      log.error(
                          "Platform event pgQueue message failed; lease will expire for retry", e);
                    }
                  }
                }));
  }
}
