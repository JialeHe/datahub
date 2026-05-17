-- Topic retention on pgQueue message rows (age / row count / total payload). Deletes apply to rows
-- regardless of consumer-group read progress. Session: SET search_path + tokens
-- __PGQUEUE_PREFIX__, __BATCH_DELETE_LIMIT__, __PGQUEUE_SCHEMA__.
-- The SET search_path clause on the function body pins the resolution to the application schema
-- so pg_cron jobs (which run with the postgres role's default search_path) can resolve unqualified
-- table/sequence references inside the function body.

CREATE OR REPLACE FUNCTION __PGQUEUE_PREFIX___apply_retention()
RETURNS void
LANGUAGE plpgsql
SET search_path = __PGQUEUE_SCHEMA__, public
AS $fn$
DECLARE
    deleted int;
    maxiter int := 50;
    i int := 0;
    batch int := __BATCH_DELETE_LIMIT__;
BEGIN
    -- Delete messages older than per-topic age retention (retention_max_age_seconds > 0).
    i := 0;
    LOOP
        i := i + 1;
        EXIT WHEN i > maxiter;

        DELETE FROM __PGQUEUE_PREFIX___message m
        WHERE m.id IN (
            SELECT ms.id
            FROM __PGQUEUE_PREFIX___message ms
            INNER JOIN __PGQUEUE_PREFIX___topic t ON t.id = ms.topic_id
            WHERE t.retention_max_age_seconds > 0
                AND ms.enqueued_at < now() - (t.retention_max_age_seconds * interval '1 second')
            LIMIT batch
        );

        GET DIAGNOSTICS deleted = ROW_COUNT;
        EXIT WHEN deleted = 0;
    END LOOP;

    -- When total row count per topic exceeds max_rows_per_topic, delete oldest rows by enqueued_at
    -- (async batched maintenance; not evaluated on INSERT — writers are never blocked by this cap).
    i := 0;
    LOOP
        i := i + 1;
        EXIT WHEN i > maxiter;

        DELETE FROM __PGQUEUE_PREFIX___message m
        WHERE m.id IN (
            SELECT ms.id
            FROM __PGQUEUE_PREFIX___message ms
            INNER JOIN __PGQUEUE_PREFIX___topic t ON t.id = ms.topic_id
            INNER JOIN (
                SELECT m2.topic_id, COUNT(*) AS c
                FROM __PGQUEUE_PREFIX___message m2
                GROUP BY m2.topic_id
            ) cnt ON cnt.topic_id = ms.topic_id
            WHERE t.max_rows_per_topic > 0
                AND cnt.c > t.max_rows_per_topic
            ORDER BY ms.enqueued_at ASC
            LIMIT batch
        );

        GET DIAGNOSTICS deleted = ROW_COUNT;
        EXIT WHEN deleted = 0;
    END LOOP;

    -- When sum(payload_bytes) per topic exceeds max_total_payload_bytes, delete oldest rows by
    -- enqueued_at (same batched pattern as row-count cap).
    i := 0;
    LOOP
        i := i + 1;
        EXIT WHEN i > maxiter;

        DELETE FROM __PGQUEUE_PREFIX___message m
        WHERE m.id IN (
            SELECT ms.id
            FROM __PGQUEUE_PREFIX___message ms
            INNER JOIN __PGQUEUE_PREFIX___topic t ON t.id = ms.topic_id
            INNER JOIN (
                SELECT m2.topic_id, COALESCE(SUM(m2.payload_bytes), 0) AS s
                FROM __PGQUEUE_PREFIX___message m2
                GROUP BY m2.topic_id
            ) tot ON tot.topic_id = ms.topic_id
            WHERE t.max_total_payload_bytes > 0
                AND tot.s > t.max_total_payload_bytes
            ORDER BY ms.enqueued_at ASC
            LIMIT batch
        );

        GET DIAGNOSTICS deleted = ROW_COUNT;
        EXIT WHEN deleted = 0;
    END LOOP;

    -- Aggressive retention: when enabled for a topic and at least one consumer is registered,
    -- delete messages where every registered consumer group has a committed offset >= enqueue_seq
    -- for the message's (topic_id, partition_id). Skip when any registered group is STUCK_AHEAD
    -- (offset_value > MAX(enqueue_seq) for that topic/partition).
    i := 0;
    LOOP
        i := i + 1;
        EXIT WHEN i > maxiter;

        DELETE FROM __PGQUEUE_PREFIX___message m
        WHERE m.id IN (
            SELECT ms.id
            FROM __PGQUEUE_PREFIX___message ms
            INNER JOIN __PGQUEUE_PREFIX___topic t ON t.id = ms.topic_id
            WHERE t.aggressive_retention = true
                AND EXISTS (
                    SELECT 1 FROM __PGQUEUE_PREFIX___consumer_registration cr
                    WHERE cr.topic_id = ms.topic_id
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM __PGQUEUE_PREFIX___consumer_registration cr_ahead
                    INNER JOIN __PGQUEUE_PREFIX___consumer_offset co_ahead
                        ON co_ahead.consumer_group = cr_ahead.consumer_group
                        AND co_ahead.topic_id = ms.topic_id
                        AND co_ahead.partition_id = ms.partition_id
                    WHERE cr_ahead.topic_id = ms.topic_id
                        AND co_ahead.offset_value > COALESCE(
                            (
                                SELECT MAX(m2.enqueue_seq)
                                FROM __PGQUEUE_PREFIX___message m2
                                WHERE m2.topic_id = ms.topic_id
                                    AND m2.partition_id = ms.partition_id
                            ),
                            0
                        )
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM __PGQUEUE_PREFIX___consumer_registration cr2
                    WHERE cr2.topic_id = ms.topic_id
                        AND NOT EXISTS (
                            SELECT 1
                            FROM __PGQUEUE_PREFIX___consumer_offset co
                            WHERE co.consumer_group = cr2.consumer_group
                                AND co.topic_id = ms.topic_id
                                AND co.partition_id = ms.partition_id
                                AND co.offset_value >= ms.enqueue_seq
                        )
                )
            LIMIT batch
        );

        GET DIAGNOSTICS deleted = ROW_COUNT;
        EXIT WHEN deleted = 0;
    END LOOP;

__PGQUEUE_APPLY_RETENTION_PARTMAN_TAIL__
END;
$fn$;
