-- Remove legacy per-message dequeue columns; Java and Python consumers use *_message_group_lease only.
-- Session: SET search_path + tokens __PGQUEUE_PREFIX__, __PGQUEUE_SCHEMA__.

ALTER TABLE IF EXISTS __PGQUEUE_SCHEMA__.__PGQUEUE_PREFIX___message
    DROP COLUMN IF EXISTS locked_until,
    DROP COLUMN IF EXISTS lock_owner;
