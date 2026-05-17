-- Consumer registration for aggressive retention.  When aggressive_retention is enabled for a
-- topic, the retention function can purge messages once all registered consumers have advanced
-- their offsets past those messages.
-- Session: SET search_path + tokens __PGQUEUE_PREFIX__, __PGQUEUE_SCHEMA__.

CREATE TABLE IF NOT EXISTS __PGQUEUE_PREFIX___consumer_registration (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumer_group text NOT NULL,
    topic_id bigint NOT NULL REFERENCES __PGQUEUE_PREFIX___topic (id) ON DELETE CASCADE,
    registered_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (consumer_group, topic_id)
);
