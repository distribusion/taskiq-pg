"""SQL for the asyncpg broker. Status literals baked in; format {table_name} at call."""

from __future__ import annotations

from taskiq_pg.status import MessageStatus

# Additive DDL: base table + idempotent ALTERs so a legacy (master) table gains
# every new column in place before the indexes below reference them.
# lock_key stays vestigial (old advisory-lock workers still read it during rollout).
CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {{table_name}} (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    message TEXT NOT NULL,
    labels JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    scheduled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT '{MessageStatus.QUEUED.value}' CHECK (status IN ('{MessageStatus.QUEUED.value}', '{MessageStatus.ACTIVE.value}', '{MessageStatus.COMPLETED.value}', '{MessageStatus.DEAD.value}')),
    lock_key SERIAL NOT NULL,
    expire_at TIMESTAMP WITH TIME ZONE,
    group_key VARCHAR,
    retry_count INTEGER DEFAULT 0,
    heartbeat_at TIMESTAMP WITH TIME ZONE
);
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT '{MessageStatus.QUEUED.value}';
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS lock_key SERIAL NOT NULL;
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS expire_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS group_key VARCHAR;
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP WITH TIME ZONE;
-- Legacy tables carry an auto-named CHECK without 'dead'. Only migrate when no
-- existing check constraint already permits 'dead' — DROP/ADD takes ACCESS EXCLUSIVE
-- and revalidates every row, so we must not run it on every startup.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{{table_name}}'::regclass
          AND contype = 'c'
          AND (SELECT attnum FROM pg_attribute
               WHERE attrelid = '{{table_name}}'::regclass
                 AND attname = 'status' AND NOT attisdropped) = ANY(conkey)
          AND pg_get_constraintdef(oid) LIKE '%dead%'
    ) THEN
        ALTER TABLE {{table_name}} DROP CONSTRAINT IF EXISTS {{table_name_safe}}_status_check;
        ALTER TABLE {{table_name}} ADD CONSTRAINT {{table_name_safe}}_status_check CHECK (status IN ('{MessageStatus.QUEUED.value}', '{MessageStatus.ACTIVE.value}', '{MessageStatus.COMPLETED.value}', '{MessageStatus.DEAD.value}'));
    END IF;
END
$do$;
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_status_scheduled ON {{table_name}} (status, scheduled_at) WHERE status = '{MessageStatus.QUEUED.value}';
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_group_key ON {{table_name}} (group_key) WHERE group_key IS NOT NULL AND status = '{MessageStatus.ACTIVE.value}';
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_expire_at ON {{table_name}} (expire_at) WHERE expire_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_active_heartbeat ON {{table_name}} (heartbeat_at) WHERE status = '{MessageStatus.ACTIVE.value}';
"""  # noqa: E501

INSERT_MESSAGE_QUERY = """
INSERT INTO {table_name}
    (task_id, task_name, message, labels, group_key, expire_at, scheduled_at)
VALUES ($1, $2, $3, $4, $5, NULL, {scheduled_at})
RETURNING id
"""

SELECT_MESSAGE_QUERY = "SELECT * FROM {table_name} WHERE id = $1"

DELETE_MESSAGE_QUERY = "DELETE FROM {table_name} WHERE id = $1"

# Two locks, two jobs. Advisory-xact lock (group_key) = group mutex: makes
# "no active row for this group? then claim it" atomic across workers, which the
# NOT IN subquery alone can't under READ COMMITTED. FOR UPDATE SKIP LOCKED = don't
# double-claim a row + let workers fan out. Ungrouped rows skip the advisory try.
# 64-bit key via hashtextextended(group_key, $1): $1 is the keyspace seed, so distinct
# groups (and keyspaces) almost never collide. Stamp heartbeat at claim (first beat).
DEQUEUE_MESSAGE_QUERY = f"""
WITH next_message AS (
    SELECT id
    FROM {{table_name}}
    WHERE status = '{MessageStatus.QUEUED.value}'
      AND scheduled_at <= NOW()
      AND (expire_at IS NULL OR expire_at > NOW())
      AND (group_key IS NULL OR group_key NOT IN (
          SELECT DISTINCT group_key
          FROM {{table_name}}
          WHERE status = '{MessageStatus.ACTIVE.value}'
            AND group_key IS NOT NULL
      ))
      AND (group_key IS NULL
           OR pg_try_advisory_xact_lock(hashtextextended(group_key, $1)))
    ORDER BY scheduled_at, created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE {{table_name}}
SET status = '{MessageStatus.ACTIVE.value}', heartbeat_at = NOW()
FROM next_message
WHERE {{table_name}}.id = next_message.id
RETURNING {{table_name}}.*
"""

# Batched liveness refresh for this process's in-flight ids. status guard avoids
# resurrecting a row already swept back to queued.
HEARTBEAT_MESSAGES_QUERY = f"""
UPDATE {{table_name}}
SET heartbeat_at = NOW()
WHERE id = ANY($1::int[]) AND status = '{MessageStatus.ACTIVE.value}'
"""

# TTL = completed-retention window.
COMPLETE_MESSAGE_QUERY = f"""
UPDATE {{table_name}}
SET status = '{MessageStatus.COMPLETED.value}', expire_at = NOW() + ($1::INTEGER * INTERVAL '1 second')
WHERE id = $2 AND status = '{MessageStatus.ACTIVE.value}'
"""  # noqa: E501

# Reclaim rows whose lease went stale (worker presumed dead). $1: timeout secs,
# $2: max_retry_attempts. A row that has burned its attempts is parked in 'dead'
# (terminal, excluded from dequeue) instead of looping forever.
SWEEP_MESSAGES_QUERY = f"""
WITH stuck_messages AS (
    SELECT id
    FROM {{table_name}}
    WHERE status = '{MessageStatus.ACTIVE.value}'
      AND heartbeat_at < NOW() - ($1::INTEGER * INTERVAL '1 second')
    ORDER BY heartbeat_at
    LIMIT 100
    FOR UPDATE SKIP LOCKED
)
UPDATE {{table_name}}
SET status = CASE
        WHEN retry_count + 1 >= $2::INTEGER THEN '{MessageStatus.DEAD.value}'
        ELSE '{MessageStatus.QUEUED.value}'
    END,
    retry_count = retry_count + 1
FROM stuck_messages
WHERE {{table_name}}.id = stuck_messages.id
RETURNING {{table_name}}.id, {{table_name}}.status
"""

CLEANUP_EXPIRED_QUERY = f"""
DELETE FROM {{table_name}}
WHERE id IN (
    SELECT id
    FROM {{table_name}}
    WHERE expire_at IS NOT NULL
      AND expire_at < NOW()
      AND status = '{MessageStatus.COMPLETED.value}'
    LIMIT 1000
)
"""
