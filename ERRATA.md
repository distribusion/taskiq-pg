# ERRATA

Known defects, deferred out of the heartbeat-lease / `FOR UPDATE SKIP LOCKED` PR.
Severity order.

1. **A stalled worker's ack breaks group serialization.** The ack marks the message
   completed without checking it still owns it, freeing the group while another
   worker runs that message — so the next message in the group starts concurrently,
   and the running one is left with no lease to reclaim it if that worker dies.
   Requires, in order: a worker's heartbeat stopping for longer than
   `stuck_message_timeout` (300s) — most plausibly a sync task blocking the event
   loop, since the heartbeat is a task on that same loop; the sweeper's next pass,
   up to `sweep_interval` (60s) later, requeueing rather than dead-lettering it,
   which needs `retry_count` below `max_retry_attempts`; another worker claiming it;
   and then the original worker finishing and acking. Delivery is unaffected —
   at-least-once still holds, the message simply runs twice.

2. **Health check on every dequeue.** `_dequeue_message` runs `SELECT 1` before every
   claim — an extra round-trip per message on the serialized `dequeue_conn`, on a
   throughput-focused branch.

3. **`_queue` grows unbounded under sustained backlog.** The queue is now only a
   wakeup signal, but every NOTIFY still `put_nowait`s while `listen()` calls
   `get()` only when a dequeue returns None. Under sustained load `get()` never
   runs and kicks accumulate permanently.
