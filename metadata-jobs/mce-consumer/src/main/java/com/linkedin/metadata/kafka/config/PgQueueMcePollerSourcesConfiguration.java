package com.linkedin.metadata.kafka.config;

import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.metadata.kafka.CDCProcessor;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.kafka.MetadataChangeEventsProcessor;
import com.linkedin.metadata.kafka.MetadataChangeProposalConsumer;
import com.linkedin.metadata.kafka.batch.BatchMetadataChangeProposalsProcessor;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.Topics;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * pgQueue worker registrations for MCE consumer — each bean is one parallel supervisor
 * (Kafka-style).
 */
@Slf4j
@Configuration
public class PgQueueMcePollerSourcesConfiguration {

  private static final int MCP_BATCH = 50;
  private static final int MCE_BATCH = 50;
  private static final int CDC_BATCH = 50;
  private static final int BATCH_MCP_POLL = 100;

  @Bean
  @Conditional(PgQueueMessagingAndMetadataChangeProposalProcessorCondition.class)
  public PgQueuePollerSource pgQueueMcpSource(
      MetadataChangeProposalConsumer consumer,
      @Value(MCP_CONSUMER_GROUP_ID_VALUE) String groupId,
      @Value("${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
          String topic) {
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                MCP_BATCH,
                "pgqueue-" + groupId,
                100,
                500,
                1000,
                (logicalTopic, batch, ctx) -> {
                  for (QueueReceivedMessage msg : batch) {
                    try {
                      GenericRecord record = ctx.decodeAvro(msg, logicalTopic);
                      InboundMetadataEnvelope<GenericRecord> envelope =
                          InboundMetadataEnvelope.fromPgQueue(msg, logicalTopic, groupId, record);
                      consumer.accept(envelope, groupId);
                      ctx.commit(List.of(msg.handle()));
                    } catch (Exception e) {
                      log.error("MCP pgQueue message failed; lease will expire for retry", e);
                    }
                  }
                }));
  }

  @Bean
  @Conditional(PgQueueMessagingAndMetadataChangeEventsProcessorCondition.class)
  public PgQueuePollerSource pgQueueMceSource(
      MetadataChangeEventsProcessor processor,
      @Value("${METADATA_CHANGE_EVENT_KAFKA_CONSUMER_GROUP_ID:mce-consumer-job-client}")
          String groupId,
      @Value(
              "${METADATA_CHANGE_EVENT_NAME:${KAFKA_MCE_TOPIC_NAME:"
                  + Topics.METADATA_CHANGE_EVENT
                  + "}}")
          String topic) {
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                MCE_BATCH,
                "pgqueue-mce-" + groupId,
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
                      log.error("MCE pgQueue message failed; lease will expire for retry", e);
                    }
                  }
                }));
  }

  @Bean
  @Conditional(PgQueueMessagingAndCdcProcessorCondition.class)
  public PgQueuePollerSource pgQueueCdcSource(
      CDCProcessor processor,
      @Value("${kafka.topics.cdcTopic.name:datahub.datahub.metadata_aspect_v2}") String topic,
      @Value("${kafka.consumer.cdcConsumerGroupId:cdc-consumer-job-client}") String groupId) {
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                CDC_BATCH,
                "pgqueue-cdc-" + groupId,
                100,
                500,
                1000,
                (logicalTopic, batch, ctx) -> {
                  for (QueueReceivedMessage msg : batch) {
                    try {
                      String json = ctx.decodeUtf8(msg);
                      processor.consumePgQueue(
                          logicalTopic,
                          msg.routingKey(),
                          json,
                          msg.handle().partitionId(),
                          msg.handle().enqueueSeq(),
                          msg.payload().length,
                          msg.handle().enqueuedAt().toEpochMilli());
                      ctx.commit(List.of(msg.handle()));
                    } catch (Exception e) {
                      log.error("CDC pgQueue message failed; lease will expire for retry", e);
                    }
                  }
                }));
  }

  @Bean
  @Conditional(PgQueueMessagingAndBatchMetadataChangeProposalProcessorCondition.class)
  public PgQueuePollerSource pgQueueBatchMcpSource(
      BatchMetadataChangeProposalsProcessor batchProcessor,
      @Value(MCP_CONSUMER_GROUP_ID_VALUE) String groupId,
      @Value("${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
          String topic) {
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                BATCH_MCP_POLL,
                "pgqueue-batch-mcp-" + groupId,
                100,
                500,
                1000,
                (logicalTopic, rawBatch, ctx) -> {
                  List<ConsumerRecord<String, GenericRecord>> synthetic =
                      new ArrayList<>(rawBatch.size());
                  List<QueueMessageHandle> handles = new ArrayList<>(rawBatch.size());
                  for (QueueReceivedMessage msg : rawBatch) {
                    GenericRecord record = ctx.decodeAvro(msg, logicalTopic);
                    var h = msg.handle();
                    synthetic.add(
                        new ConsumerRecord<>(
                            logicalTopic,
                            h.partitionId(),
                            h.enqueueSeq(),
                            msg.routingKey(),
                            record));
                    handles.add(h);
                  }
                  try {
                    batchProcessor.consume(synthetic);
                    ctx.commit(handles);
                  } catch (Exception e) {
                    log.error("Batch MCP pgQueue processing failed; leases expire for retry", e);
                  }
                }));
  }
}
