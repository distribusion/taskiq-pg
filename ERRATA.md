# ERRATA

Known defects, deferred out of the heartbeat-lease / `FOR UPDATE SKIP LOCKED` PR.
Severity order.

1. **Group mutex not honored across processes.** The `group_key NOT IN (SELECT ... active)`
   dequeue subquery is snapshot-based (READ COMMITTED). Two worker processes pick
   different queued rows of the same group before either commits `active`, and both
   claim. Concurrent-execution guarantee holds single-process only; the test uses
   one connection and misses it.

2. **No in-group ordering.** Only mutual exclusion is (partially) attempted; nothing
   guarantees FIFO within a group. Under the #1 race the newer row can go active
   first, and `delay` labels reorder by `scheduled_at`.

3. **`max_retry_attempts` is dead.** `retry_count` is only incremented in the sweep,
   never compared to `max_retry_attempts` (read nowhere). Poison messages loop
   forever — no dead-letter, no parking.

4. **Health check on every dequeue.** `_dequeue_message` runs `SELECT 1` before every
   claim — an extra round-trip per message on the serialized `dequeue_conn`, on a
   throughput-focused branch.

5. **`_queue` grows unbounded under sustained backlog.** The queue is now only a
   wakeup signal, but every NOTIFY still `put_nowait`s while `listen()` calls
   `get()` only when a dequeue returns None. Under sustained load `get()` never
   runs and kicks accumulate permanently.
