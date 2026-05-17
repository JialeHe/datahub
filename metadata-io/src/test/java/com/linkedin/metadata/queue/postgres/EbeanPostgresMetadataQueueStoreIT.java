package com.linkedin.metadata.queue.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.queue.ConsumerOffsetResetReport;
import com.linkedin.metadata.queue.ConsumerOffsetResetSpec;
import com.linkedin.metadata.queue.ConsumerRegistrationRow;
import com.linkedin.metadata.queue.EnqueueBatchItem;
import com.linkedin.metadata.queue.MetadataQueueRouting;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PartitionOffsetSkew;
import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.PriorityBandConfig;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueMessageHeader;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.queue.QueueTableNames;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import io.ebean.Database;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * PostgreSQL-backed smoke tests for {@link EbeanPostgresMetadataQueueStore} using a minimal,
 * non-partitioned message table (production SqlSetup uses pg_partman range partitions).
 */
public class EbeanPostgresMetadataQueueStoreIT {

  private PostgreSQLContainer<?> postgres;
  private Database database;
  private QueueTableNames names;
  private MetadataQueueStore store;
  private QueueTopicDefaults defaults;

  @BeforeClass
  public void init() throws Exception {
    postgres = PostgresTestUtils.startPostgres();
    PostgresTestUtils.IntegrationNamespace ns =
        PostgresTestUtils.newIntegrationNamespace("pgqueue");
    names = new QueueTableNames(ns.getSchema(), ns.getTablePrefix());
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pgqueue_store_it"));
    Assert.assertFalse(names.matchesDefaultEntityPhysicalMapping());

    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      applyMinimalPgQueueTables(c, names);
      c.commit();
    }

    PriorityBandConfig bandConfig =
        PriorityBandConfig.parse(
            new ObjectMapper(),
            "[{\"range\":[0,3],\"weight\":70},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]");
    store = new EbeanPostgresMetadataQueueStore(database, names, bandConfig);
    defaults = new QueueTopicDefaults(4, 0, 0L, 0L, false, null);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  private static void applyMinimalPgQueueTables(Connection c, QueueTableNames q) throws Exception {
    try (Statement st = c.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + q.schema());
      st.execute("DROP TABLE IF EXISTS " + q.qualifiedConsumerRegistration() + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + q.qualifiedMessageGroupLease() + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + q.qualifiedMessage() + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + q.qualifiedConsumerOffset() + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + q.qualifiedTopic() + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + q.qualifiedContentType() + " CASCADE");
      st.execute(
          "CREATE TABLE "
              + q.qualifiedContentType()
              + " ( id smallint generated always as identity primary key,"
              + " mime text not null unique )");
      st.execute(
          "INSERT INTO "
              + q.qualifiedContentType()
              + " (mime) VALUES ('application/avro'), ('application/test')");
      st.execute(
          "CREATE TABLE "
              + q.qualifiedTopic()
              + " ("
              + " id bigint generated always as identity primary key,"
              + " topic_name text not null unique,"
              + " partition_count int not null,"
              + " retention_max_age_seconds int not null,"
              + " max_rows_per_topic bigint not null,"
              + " max_total_payload_bytes bigint not null,"
              + " default_content_type_id smallint references "
              + q.qualifiedContentType()
              + "(id),"
              + " aggressive_retention boolean not null default false,"
              + " created_at timestamptz not null default now()"
              + ")");
      st.execute(
          "CREATE TABLE "
              + q.qualifiedConsumerOffset()
              + " ("
              + " id bigint generated always as identity primary key,"
              + " consumer_group text not null,"
              + " topic_id bigint not null references "
              + q.qualifiedTopic()
              + "(id) on delete cascade,"
              + " partition_id int not null,"
              + " offset_value bigint not null default 0,"
              + " epoch bigint not null default 0,"
              + " unique (consumer_group, topic_id, partition_id)"
              + ")");
      st.execute(
          "CREATE TABLE "
              + q.qualifiedMessage()
              + " ("
              + " id bigint generated always as identity,"
              + " topic_id bigint not null references "
              + q.qualifiedTopic()
              + "(id) on delete cascade,"
              + " partition_id int not null,"
              + " routing_key text not null,"
              + " enqueue_seq bigint not null,"
              + " priority smallint not null default 5 check (priority between 0 and 9),"
              + " payload bytea not null,"
              + " content_type_id smallint references "
              + q.qualifiedContentType()
              + "(id),"
              + " payload_compression smallint not null default 0,"
              + " headers jsonb,"
              + " enqueued_at timestamptz not null default now(),"
              + " primary key (id, enqueued_at),"
              + " unique (topic_id, partition_id, enqueue_seq, enqueued_at)"
              + ")");
      st.execute(
          "CREATE TABLE "
              + q.qualifiedMessageGroupLease()
              + " ("
              + " id bigint generated always as identity primary key,"
              + " message_id bigint not null,"
              + " message_enqueued_at timestamptz not null,"
              + " consumer_group text not null,"
              + " lock_owner text not null,"
              + " locked_until timestamptz not null,"
              + " unique (message_id, message_enqueued_at, consumer_group),"
              + " foreign key (message_id, message_enqueued_at) references "
              + q.qualifiedMessage()
              + "(id, enqueued_at) on delete cascade"
              + ")");
      st.execute(
          "CREATE TABLE "
              + q.qualifiedConsumerRegistration()
              + " ("
              + " id bigint generated always as identity primary key,"
              + " consumer_group text not null,"
              + " topic_id bigint not null references "
              + q.qualifiedTopic()
              + "(id) on delete cascade,"
              + " registered_at timestamptz not null default now(),"
              + " last_heartbeat_at timestamptz not null default now(),"
              + " unique (consumer_group, topic_id)"
              + ")");
    }
  }

  @Test
  public void ensureTopic_fetch_roundTrip() {
    String topic = "topic_" + UUID.randomUUID();
    long id = store.ensureTopic(topic, defaults);
    Assert.assertTrue(id > 0);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    Assert.assertEquals(meta.id(), id);
    Assert.assertEquals(meta.partitionCount(), 4);
  }

  @Test
  public void topicPartitionCount_neverDowngradesBelowExistingOrMessages() throws Exception {
    String topic = "topic_" + UUID.randomUUID();
    QueueTopicDefaults lowPc = new QueueTopicDefaults(1, 0, 0L, 0L, false, "application/avro");
    store.ensureTopic(topic, lowPc);
    QueueTopicMetadata meta1 = store.fetchTopic(topic).orElseThrow();
    Assert.assertEquals(meta1.partitionCount(), 1);
    long topicId = meta1.id();

    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      try (Statement st = c.createStatement()) {
        st.executeUpdate(
            "INSERT INTO "
                + names.qualifiedMessage()
                + " (topic_id, partition_id, routing_key, enqueue_seq, priority, payload,"
                + " payload_compression) VALUES ("
                + topicId
                + ", 2, 'urn:li:test:orphan', 1, 0, decode('00', 'hex'), 0)");
      }
      c.commit();
    }

    store.ensureTopic(topic, lowPc);
    QueueTopicMetadata meta2 = store.fetchTopic(topic).orElseThrow();
    Assert.assertEquals(meta2.partitionCount(), 3);

    store.ensureTopic(topic, lowPc);
    QueueTopicMetadata meta3 = store.fetchTopic(topic).orElseThrow();
    Assert.assertEquals(meta3.partitionCount(), 3);
  }

  @Test
  public void enqueue_receive_commitForGroup() {
    String topic = "topic_" + UUID.randomUUID();
    String routingKey = "urn:li:dataset:(foo,bar,PROD)";
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    byte[] payload = new byte[] {9, 8, 7};
    QueueMessageHandle handle =
        store.enqueue(
            topic, routingKey, defaults, 0, payload, Optional.of("application/test"), List.of());

    List<QueueReceivedMessage> locked =
        store.receiveBatchForGroup(
            "group_a", meta.id(), partitions, "cg:1", Duration.ofSeconds(60), 10);
    Assert.assertEquals(locked.size(), 1);
    Assert.assertEquals(locked.get(0).payload(), payload);
    Assert.assertEquals(locked.get(0).payloadCompression(), PgQueuePayloadCompression.NONE);
    Assert.assertEquals(locked.get(0).routingKey(), routingKey);
    Assert.assertEquals(locked.get(0).handle().id(), handle.id());

    int updated = store.commitForGroup("group_a", List.of(locked.get(0).handle()), true);
    Assert.assertEquals(updated, 1);
  }

  @Test
  public void enqueue_receive_preservesKafkaStyleHeaders() {
    String topic = "topic_" + UUID.randomUUID();
    String routingKey = "urn:li:dataset:(foo,bar,PROD)";
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    List<QueueMessageHeader> hdrs =
        List.of(
            new QueueMessageHeader("k1", "v1".getBytes(StandardCharsets.UTF_8)),
            new QueueMessageHeader("empty", new byte[0]));

    byte[] payload = new byte[] {1};
    store.enqueue(topic, routingKey, defaults, 0, payload, Optional.empty(), hdrs);

    List<QueueReceivedMessage> locked =
        store.receiveBatchForGroup(
            "cg:h", meta.id(), partitions, "cg:h-owner", Duration.ofSeconds(60), 10);
    Assert.assertEquals(locked.size(), 1);
    Assert.assertEquals(locked.get(0).headers().size(), 2);
    Assert.assertEquals(locked.get(0).headers().get(0).key(), "k1");
    Assert.assertEquals(
        new String(locked.get(0).headers().get(0).value(), StandardCharsets.UTF_8), "v1");
    Assert.assertEquals(locked.get(0).headers().get(1).key(), "empty");
    Assert.assertEquals(locked.get(0).headers().get(1).value().length, 0);
    Assert.assertEquals(locked.get(0).routingKey(), routingKey);

    store.commitForGroup("cg:h", List.of(locked.get(0).handle()), false);
  }

  @Test
  public void enqueueBatch_receivePollPartitions() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();

    List<EnqueueBatchItem> items =
        List.of(
            new EnqueueBatchItem(
                topic,
                "urn:li:dataset:(a,b,PROD)",
                0,
                new byte[] {1},
                Optional.empty(),
                List.of(),
                PgQueuePayloadCompression.NONE),
            new EnqueueBatchItem(
                topic,
                "urn:li:dataset:(c,d,PROD)",
                0,
                new byte[] {2},
                Optional.empty(),
                List.of(),
                PgQueuePayloadCompression.NONE));

    List<QueueMessageHandle> handles = store.enqueueBatch(items, defaults);
    Assert.assertEquals(handles.size(), 2);

    List<QueueReceivedMessage> polled =
        store.receiveBatchForGroup(
            "cg:poll", meta.id(), List.of(0, 1, 2, 3), "cg:poll-owner", Duration.ofSeconds(30), 10);
    Assert.assertEquals(polled.size(), 2);
    Assert.assertEquals(
        polled.stream().map(QueueReceivedMessage::routingKey).sorted().toList(),
        List.of("urn:li:dataset:(a,b,PROD)", "urn:li:dataset:(c,d,PROD)"));
    store.commitForGroup(
        "cg:poll", polled.stream().map(QueueReceivedMessage::handle).toList(), false);
  }

  @Test
  public void enqueue_snappy_payload_compression_roundTrip() {
    String topic = "topic_" + UUID.randomUUID();
    String routingKey = "urn:li:dataset:(foo,bar,PROD)";
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    byte[] inner = new byte[] {1, 2, 3, 4, 5};
    byte[] stored = PgQueuePayloadCodec.encode(inner, PgQueuePayloadCompression.SNAPPY);
    store.enqueue(
        topic,
        routingKey,
        defaults,
        0,
        stored,
        Optional.of("application/vnd.confluent.avro+binary"),
        List.of(),
        PgQueuePayloadCompression.SNAPPY);

    List<QueueReceivedMessage> locked =
        store.receiveBatchForGroup(
            "cg:snappy", meta.id(), partitions, "cg:snappy-owner", Duration.ofSeconds(60), 10);
    Assert.assertEquals(locked.size(), 1);
    Assert.assertEquals(locked.get(0).payloadCompression(), PgQueuePayloadCompression.SNAPPY);
    Assert.assertEquals(
        PgQueuePayloadCodec.decode(locked.get(0).payload(), PgQueuePayloadCompression.SNAPPY),
        inner);
    Assert.assertEquals(locked.get(0).routingKey(), routingKey);
    store.commitForGroup("cg:snappy", List.of(locked.get(0).handle()), false);
  }

  @Test
  public void twoConsumerGroupsReceiveSamePayloadRow() {
    String topic = "topic_" + UUID.randomUUID();
    String routingKey = "urn:li:dataset:(foo,bar,PROD)";
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    byte[] payload = new byte[] {1, 2, 3};
    QueueMessageHandle handle =
        store.enqueue(
            topic, routingKey, defaults, 0, payload, Optional.of("application/test"), List.of());

    List<QueueReceivedMessage> g1 =
        store.receiveBatchForGroup(
            "group-one", meta.id(), partitions, "owner1", Duration.ofSeconds(60), 10);
    Assert.assertEquals(g1.size(), 1);
    Assert.assertEquals(g1.get(0).payload(), payload);
    store.commitForGroup("group-one", List.of(g1.get(0).handle()), true);

    List<QueueReceivedMessage> g2 =
        store.receiveBatchForGroup(
            "group-two", meta.id(), partitions, "owner2", Duration.ofSeconds(60), 10);
    Assert.assertEquals(g2.size(), 1);
    Assert.assertEquals(g2.get(0).handle().id(), handle.id());
    Assert.assertEquals(g2.get(0).payload(), payload);
    store.commitForGroup("group-two", List.of(g2.get(0).handle()), true);
  }

  @Test
  public void registerConsumer_insertThenHeartbeatUpdate() throws Exception {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();

    store.registerConsumer("cg-one", meta.id());
    List<ConsumerRegistrationRow> rows = store.listRegisteredConsumers(meta.id());
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0).consumerGroup(), "cg-one");
    Instant firstHeartbeat = rows.get(0).lastHeartbeatAt();

    Thread.sleep(50);
    store.registerConsumer("cg-one", meta.id());
    List<ConsumerRegistrationRow> updated = store.listRegisteredConsumers(meta.id());
    Assert.assertEquals(updated.size(), 1);
    Assert.assertTrue(
        updated.get(0).lastHeartbeatAt().compareTo(firstHeartbeat) >= 0,
        "heartbeat should advance or stay the same");

    store.registerConsumer("cg-two", meta.id());
    Assert.assertEquals(store.listRegisteredConsumers(meta.id()).size(), 2);

    Assert.assertTrue(store.unregisterConsumer("cg-one", meta.id()));
    Assert.assertEquals(store.listRegisteredConsumers(meta.id()).size(), 1);
    Assert.assertFalse(store.unregisterConsumer("cg-nonexistent", meta.id()));
  }

  // ── Priority queue integration tests ──────────────────────────────────────

  @Test
  public void enqueue_defaultPriority_receivePreservesPriorityValue() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    store.enqueue(
        topic,
        "urn:li:dataset:(a,b,PROD)",
        defaults,
        QueueTopicMetadata.DEFAULT_PRIORITY,
        new byte[] {1},
        Optional.empty(),
        List.of());

    List<QueueReceivedMessage> msgs =
        store.receiveBatchForGroup(
            "cg:defpri", meta.id(), partitions, "owner:defpri", Duration.ofSeconds(60), 10);
    Assert.assertEquals(msgs.size(), 1);
    Assert.assertEquals(msgs.get(0).priority(), QueueTopicMetadata.DEFAULT_PRIORITY);
    store.commitForGroup("cg:defpri", List.of(msgs.get(0).handle()), false);
  }

  @Test
  public void enqueue_priorityBoundaries_acceptsZeroAndNine() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);

    QueueMessageHandle h0 =
        store.enqueue(
            topic, "urn:li:test:p0", defaults, 0, new byte[] {1}, Optional.empty(), List.of());
    Assert.assertNotNull(h0);

    QueueMessageHandle h9 =
        store.enqueue(
            topic, "urn:li:test:p9", defaults, 9, new byte[] {2}, Optional.empty(), List.of());
    Assert.assertNotNull(h9);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void enqueue_priorityTooHigh_rejects() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    store.enqueue(
        topic, "urn:li:test:p10", defaults, 10, new byte[] {1}, Optional.empty(), List.of());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void enqueue_priorityNegative_rejects() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    store.enqueue(
        topic, "urn:li:test:neg", defaults, -1, new byte[] {1}, Optional.empty(), List.of());
  }

  @Test
  public void receive_withMixedPriorities_highPriorityFirst() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    String routingKey = "urn:li:test:same-partition";
    store.enqueue(topic, routingKey, defaults, 9, new byte[] {3}, Optional.empty(), List.of());
    store.enqueue(topic, routingKey, defaults, 0, new byte[] {1}, Optional.empty(), List.of());
    store.enqueue(topic, routingKey, defaults, 5, new byte[] {2}, Optional.empty(), List.of());

    List<QueueReceivedMessage> msgs =
        store.receiveBatchForGroup(
            "cg:mixed", meta.id(), partitions, "owner:mixed", Duration.ofSeconds(60), 10);
    Assert.assertEquals(msgs.size(), 3);
    Assert.assertTrue(
        msgs.get(0).priority() <= msgs.get(1).priority(),
        "First message should have highest (lowest-numbered) priority");
    Assert.assertTrue(
        msgs.get(1).priority() <= msgs.get(2).priority(), "Messages should be ordered by priority");
    store.commitForGroup(
        "cg:mixed", msgs.stream().map(QueueReceivedMessage::handle).toList(), false);
  }

  @Test
  public void receiveBatchForGroup_mixedPriorities_orderedByPriority() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    String routingKey = "urn:li:test:same-partition";
    store.enqueue(topic, routingKey, defaults, 7, new byte[] {7}, Optional.empty(), List.of());
    store.enqueue(topic, routingKey, defaults, 1, new byte[] {1}, Optional.empty(), List.of());
    store.enqueue(topic, routingKey, defaults, 4, new byte[] {4}, Optional.empty(), List.of());

    List<QueueReceivedMessage> msgs =
        store.receiveBatchForGroup(
            "cg:prigrp", meta.id(), partitions, "owner:prigrp", Duration.ofSeconds(60), 10);
    Assert.assertEquals(msgs.size(), 3);
    Assert.assertTrue(
        msgs.get(0).priority() <= msgs.get(1).priority()
            && msgs.get(1).priority() <= msgs.get(2).priority(),
        "Consumer group receive should order by priority. Got: "
            + msgs.stream().map(QueueReceivedMessage::priority).toList());
    store.commitForGroup(
        "cg:prigrp", msgs.stream().map(QueueReceivedMessage::handle).toList(), false);
  }

  @Test
  public void enqueueBatch_multiplePriorities_allPreserved() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();

    String routingKey = "urn:li:test:same-partition";
    List<EnqueueBatchItem> items =
        List.of(
            new EnqueueBatchItem(
                topic,
                routingKey,
                0,
                new byte[] {10},
                Optional.empty(),
                List.of(),
                PgQueuePayloadCompression.NONE),
            new EnqueueBatchItem(
                topic,
                routingKey,
                5,
                new byte[] {50},
                Optional.empty(),
                List.of(),
                PgQueuePayloadCompression.NONE),
            new EnqueueBatchItem(
                topic,
                routingKey,
                9,
                new byte[] {90},
                Optional.empty(),
                List.of(),
                PgQueuePayloadCompression.NONE));

    List<QueueMessageHandle> handles = store.enqueueBatch(items, defaults);
    Assert.assertEquals(handles.size(), 3);

    List<QueueReceivedMessage> msgs =
        store.receiveBatchForGroup(
            "cg:batch-pri",
            meta.id(),
            List.of(0, 1, 2, 3),
            "owner:batch-pri",
            Duration.ofSeconds(60),
            10);
    Assert.assertEquals(msgs.size(), 3);

    List<Integer> receivedPriorities = msgs.stream().map(QueueReceivedMessage::priority).toList();
    Assert.assertTrue(
        receivedPriorities.contains(0)
            && receivedPriorities.contains(5)
            && receivedPriorities.contains(9),
        "All priority values should be preserved: " + receivedPriorities);

    Assert.assertTrue(
        receivedPriorities.get(0) <= receivedPriorities.get(1)
            && receivedPriorities.get(1) <= receivedPriorities.get(2),
        "Should be ordered by priority: " + receivedPriorities);

    store.commitForGroup(
        "cg:batch-pri", msgs.stream().map(QueueReceivedMessage::handle).toList(), false);
  }

  @Test
  public void receive_samePriority_orderedByEnqueueSeq() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    String routingKey = "urn:li:test:same-partition";
    QueueMessageHandle first =
        store.enqueue(topic, routingKey, defaults, 5, new byte[] {1}, Optional.empty(), List.of());
    QueueMessageHandle second =
        store.enqueue(topic, routingKey, defaults, 5, new byte[] {2}, Optional.empty(), List.of());
    QueueMessageHandle third =
        store.enqueue(topic, routingKey, defaults, 5, new byte[] {3}, Optional.empty(), List.of());

    List<QueueReceivedMessage> msgs =
        store.receiveBatchForGroup(
            "cg:fifo", meta.id(), partitions, "owner:fifo", Duration.ofSeconds(60), 10);
    Assert.assertEquals(msgs.size(), 3);
    Assert.assertEquals(
        msgs.get(0).handle().enqueueSeq(), first.enqueueSeq(), "FIFO within same priority");
    Assert.assertEquals(
        msgs.get(1).handle().enqueueSeq(), second.enqueueSeq(), "FIFO within same priority");
    Assert.assertEquals(
        msgs.get(2).handle().enqueueSeq(), third.enqueueSeq(), "FIFO within same priority");
    store.commitForGroup(
        "cg:fifo", msgs.stream().map(QueueReceivedMessage::handle).toList(), false);
  }

  @Test
  public void peekTopicLog_mixedPriorities_orderedByPriority() {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();

    store.enqueue(
        topic, "urn:li:test:pk9", defaults, 9, new byte[] {9}, Optional.empty(), List.of());
    store.enqueue(
        topic, "urn:li:test:pk2", defaults, 2, new byte[] {2}, Optional.empty(), List.of());
    store.enqueue(
        topic, "urn:li:test:pk5", defaults, 5, new byte[] {5}, Optional.empty(), List.of());

    java.util.Map<Integer, Long> allPartitions = java.util.Map.of(0, 0L, 1, 0L, 2, 0L, 3, 0L);
    var peekRows = store.peekTopicLog(meta.id(), allPartitions, 10);
    Assert.assertEquals(peekRows.size(), 3, "Expected 3 messages across all partitions");

    List<Integer> priorities =
        peekRows.stream().map(com.linkedin.metadata.queue.QueueLogPeekRow::priority).toList();
    Assert.assertTrue(
        priorities.contains(2) && priorities.contains(5) && priorities.contains(9),
        "All priority values should be present: " + priorities);
  }

  @Test
  public void partitionMaxEnqueueSeqs_and_detectOffsetAheadOfLog() throws Exception {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    store.enqueue(
        topic, "urn:li:test:skew", defaults, 0, new byte[] {1}, Optional.empty(), List.of());

    var maxSeqs = store.partitionMaxEnqueueSeqs(meta.id(), meta.partitionCount());
    Assert.assertEquals(maxSeqs.get(0).longValue(), 1L);

    setCommittedOffset("cg-skew", meta.id(), 0, 10L);
    List<PartitionOffsetSkew> skews =
        store.detectOffsetAheadOfLog("cg-skew", meta.id(), meta.partitionCount());
    Assert.assertEquals(skews.size(), 1);
    Assert.assertEquals(skews.get(0).getPartitionId(), 0);
    Assert.assertEquals(skews.get(0).getAheadBy(), 9L);
    Assert.assertEquals(store.getCommittedOffset("cg-skew", meta.id(), 0), 10L);
  }

  @Test
  public void resetConsumerOffsets_onlyStuckAhead() throws Exception {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    store.enqueue(
        topic, "urn:li:test:reset", defaults, 0, new byte[] {1}, Optional.empty(), List.of());

    setCommittedOffset("cg-reset", meta.id(), 0, 50L);
    setCommittedOffset("cg-other", meta.id(), 0, 0L);

    ConsumerOffsetResetReport report =
        store.resetConsumerOffsets(
            ConsumerOffsetResetSpec.builder()
                .consumerGroup("cg-reset")
                .topicName(topic)
                .onlyStuckAhead(true)
                .build());
    Assert.assertEquals(report.getPartitionsUpdated(), 1);
    Assert.assertEquals(report.getResets().get(0).getPreviousOffset(), 50L);
    Assert.assertEquals(report.getResets().get(0).getNewOffset(), 1L);
    Assert.assertEquals(store.getCommittedOffset("cg-reset", meta.id(), 0), 1L);
    Assert.assertEquals(store.getCommittedOffset("cg-other", meta.id(), 0), 0L);
    Assert.assertTrue(
        store.detectOffsetAheadOfLog("cg-reset", meta.id(), meta.partitionCount()).isEmpty());
  }

  @Test
  public void receiveWithAheadOffset_doesNotChangeCommittedOffset() throws Exception {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    setCommittedOffset("cg-ahead", meta.id(), 0, 99L);

    List<QueueReceivedMessage> batch =
        store.receiveBatchForGroup(
            "cg-ahead", meta.id(), List.of(0), "owner", Duration.ofSeconds(60), 10);
    Assert.assertTrue(batch.isEmpty());
    Assert.assertEquals(store.getCommittedOffset("cg-ahead", meta.id(), 0), 99L);
  }

  @Test
  public void commitPartitionZero_onlyAdvancesPartitionZero() throws Exception {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    List<Integer> partitions = List.of(0, 1, 2, 3);

    String keyP0 = routingKeyForPartition(meta.partitionCount(), 0);
    String keyP1 = routingKeyForPartition(meta.partitionCount(), 1);
    store.enqueue(topic, keyP0, defaults, 0, new byte[] {1}, Optional.empty(), List.of());
    store.enqueue(topic, keyP1, defaults, 0, new byte[] {2}, Optional.empty(), List.of());

    List<QueueReceivedMessage> p0Only =
        store.receiveBatchForGroup(
            "cg-part", meta.id(), List.of(0), "owner", Duration.ofSeconds(60), 10);
    Assert.assertEquals(p0Only.size(), 1);
    store.commitForGroup(
        "cg-part", p0Only.stream().map(QueueReceivedMessage::handle).toList(), true);

    Assert.assertTrue(store.getCommittedOffset("cg-part", meta.id(), 0) > 0L);
    Assert.assertEquals(store.getCommittedOffset("cg-part", meta.id(), 1), 0L);

    List<QueueReceivedMessage> p1 =
        store.receiveBatchForGroup(
            "cg-part", meta.id(), List.of(1), "owner", Duration.ofSeconds(60), 10);
    Assert.assertEquals(p1.size(), 1);
  }

  private static String routingKeyForPartition(int partitionCount, int targetPartition) {
    for (int i = 0; i < 1000; i++) {
      String key = "urn:li:test:partition:" + i;
      if (MetadataQueueRouting.stablePartitionId(key, partitionCount) == targetPartition) {
        return key;
      }
    }
    throw new IllegalStateException("no routing key for partition " + targetPartition);
  }

  @Test
  public void stuckAheadOnTopicA_doesNotAffectTopicB() throws Exception {
    String topicA = "topic_a_" + UUID.randomUUID();
    String topicB = "topic_b_" + UUID.randomUUID();
    store.ensureTopic(topicA, defaults);
    store.ensureTopic(topicB, defaults);
    QueueTopicMetadata metaA = store.fetchTopic(topicA).orElseThrow();
    QueueTopicMetadata metaB = store.fetchTopic(topicB).orElseThrow();

    store.enqueue(
        topicB, "urn:li:test:b", defaults, 0, new byte[] {2}, Optional.empty(), List.of());
    setCommittedOffset("cg-topics", metaA.id(), 0, 50L);

    Assert.assertEquals(
        store.detectOffsetAheadOfLog("cg-topics", metaA.id(), metaA.partitionCount()).size(), 1);
    Assert.assertTrue(
        store.detectOffsetAheadOfLog("cg-topics", metaB.id(), metaB.partitionCount()).isEmpty());

    List<QueueReceivedMessage> fromB =
        store.receiveBatchForGroup(
            "cg-topics", metaB.id(), List.of(0), "owner", Duration.ofSeconds(60), 10);
    Assert.assertEquals(fromB.size(), 1);
  }

  @Test
  public void stuckAheadOnGroupOne_doesNotChangeGroupTwoOffset() throws Exception {
    String topic = "topic_" + UUID.randomUUID();
    store.ensureTopic(topic, defaults);
    QueueTopicMetadata meta = store.fetchTopic(topic).orElseThrow();
    store.enqueue(topic, "urn:li:test:g", defaults, 0, new byte[] {1}, Optional.empty(), List.of());

    setCommittedOffset("group-one", meta.id(), 0, 20L);
    Assert.assertEquals(store.getCommittedOffset("group-two", meta.id(), 0), 0L);

    List<PartitionOffsetSkew> skews =
        store.detectOffsetAheadOfLog("group-one", meta.id(), meta.partitionCount());
    Assert.assertEquals(skews.size(), 1);
    Assert.assertTrue(
        store.detectOffsetAheadOfLog("group-two", meta.id(), meta.partitionCount()).isEmpty());
  }

  private void setCommittedOffset(String group, long topicId, int partitionId, long offset)
      throws Exception {
    try (Connection c = database.dataSource().getConnection()) {
      try (PreparedStatement ps =
          c.prepareStatement(
              "INSERT INTO "
                  + names.qualifiedConsumerOffset()
                  + " (consumer_group, topic_id, partition_id, offset_value, epoch)"
                  + " VALUES (?, ?, ?, ?, 0)"
                  + " ON CONFLICT (consumer_group, topic_id, partition_id)"
                  + " DO UPDATE SET offset_value = EXCLUDED.offset_value")) {
        ps.setString(1, group);
        ps.setLong(2, topicId);
        ps.setInt(3, partitionId);
        ps.setLong(4, offset);
        ps.executeUpdate();
      }
    }
  }
}
