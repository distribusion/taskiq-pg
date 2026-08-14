# ERRATA

Known defects, deferred out of the heartbeat-lease / `FOR UPDATE SKIP LOCKED` PR.
Severity order.

1. **No in-group ordering.** Only mutual exclusion is attempted; nothing
   guarantees FIFO within a group. `delay` labels reorder by `scheduled_at`.

2. **`max_retry_attempts` is dead.** `retry_count` is only incremented in the sweep,
   never compared to `max_retry_attempts` (read nowhere). Poison messages loop
   forever — no dead-letter, no parking.

3. **Health check on every dequeue.** `_dequeue_message` runs `SELECT 1` before every
   claim — an extra round-trip per message on the serialized `dequeue_conn`, on a
   throughput-focused branch.

4. **`_queue` grows unbounded under sustained backlog.** The queue is now only a
   wakeup signal, but every NOTIFY still `put_nowait`s while `listen()` calls
   `get()` only when a dequeue returns None. Under sustained load `get()` never
   runs and kicks accumulate permanently.
