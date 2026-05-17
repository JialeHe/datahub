-- Per-consumer-group leases for Kafka-style parallel consumer groups on one message payload row.
-- Session: SET search_path + tokens __PGQUEUE_PREFIX__, __PGQUEUE_SCHEMA__.

CREATE TABLE IF NOT EXISTS __PGQUEUE_PREFIX___message_group_lease (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id bigint NOT NULL,
    message_enqueued_at timestamptz NOT NULL,
    consumer_group text NOT NULL,
    lock_owner text NOT NULL,
    locked_until timestamptz NOT NULL,
    CONSTRAINT __PGQUEUE_PREFIX___message_group_lease_unique_msg_group UNIQUE (
        message_id,
        message_enqueued_at,
        consumer_group
    ),
    CONSTRAINT __PGQUEUE_PREFIX___message_group_lease_msg_fk FOREIGN KEY (message_id, message_enqueued_at)
        REFERENCES __PGQUEUE_PREFIX___message (id, enqueued_at)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx___PGQUEUE_PREFIX___message_group_lease_group_unlocked
    ON __PGQUEUE_PREFIX___message_group_lease (consumer_group, locked_until);
