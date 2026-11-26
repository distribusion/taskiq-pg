"""Throughput stress test for the taskiq-pg broker.

This module is both the worker import target (``bench.throughput:broker``) and
the benchmark driver (``python -m bench.throughput``). The driver spawns a real
``taskiq worker``, enqueues N tasks, and times how long the worker takes to
drain them.

DSN/table/channel are read from ``BENCH_*`` env vars so the spawned worker
rebuilds an identical broker:

* ``BENCH_DSN``           - PostgreSQL DSN (default: POSTGRESQL_URL or localhost:25432)
* ``BENCH_TABLE``         - messages table name (default: random ``bench_<rand>``)
* ``BENCH_CHANNEL``       - LISTEN/NOTIFY channel (default: ``<table>_ch``)
* ``BENCH_BUSY_SECONDS``  - CPU busy-loop seconds for CPU-bound tasks (default: 0.005)
* ``BENCH_SLEEP_SECONDS`` - asyncio.sleep seconds for I/O-bound tasks (default: 0.005)
* ``BENCH_FAIL_RATE``     - per-attempt failure probability (default: 0, no retries)
* ``BENCH_MAX_RETRIES``   - attempt budget per message (default: 3)
* ``BENCH_RETRY_DELAY``   - backoff seconds on retry (default: 0)

This broker claims each message atomically (``FOR UPDATE SKIP LOCKED``), so a
task is executed exactly once regardless of ``--workers``. Use ``--workers > 1``
to measure concurrent drain across competing worker processes.

``--groups N`` spreads the tasks over N ``group_key`` values, which caps
concurrency at N (one active message per group). Adding ``--ordered`` also
enforces FIFO within each group; compare the two at the same ``--groups`` to
price the head-of-line predicate.

``--fail-rate P`` makes each attempt fail with probability P and attaches
``OrderedRetryMiddleware``, so failures requeue the same row instead of kicking a
new one. Compare against ``--fail-rate 0`` at the same ``-n`` to price retries;
the report separates messages drained from attempts executed.

``--producers N`` kicks from N separate non-worker brokers. ``--duration S`` kicks
``--rate`` tasks per second from each of them for S seconds instead of ``-n``, for a
soak. Throughout, the report tracks the longest time work was outstanding with no
completion; a stall over ``--stall-timeout`` exits 1.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import random
import signal
import string
import sys
import time

from taskiq_pg.broker import AsyncpgBroker
from taskiq_pg.middlewares import OrderedRetryMiddleware

DSN = os.environ.setdefault(
    "BENCH_DSN",
    os.environ.get("POSTGRESQL_URL")
    or "postgresql://postgres:postgres@localhost:25432/postgres",
)
TABLE = os.environ.setdefault(
    "BENCH_TABLE",
    "bench_" + "".join(random.choice(string.ascii_lowercase) for _ in range(8)),
)
CHANNEL = os.environ.setdefault("BENCH_CHANNEL", f"{TABLE}_ch")
BUSY_SECONDS = float(os.environ.setdefault("BENCH_BUSY_SECONDS", "0.005"))
SLEEP_SECONDS = float(os.environ.setdefault("BENCH_SLEEP_SECONDS", "0.005"))
FAIL_RATE = float(os.environ.setdefault("BENCH_FAIL_RATE", "0"))
MAX_RETRIES = int(os.environ.setdefault("BENCH_MAX_RETRIES", "3"))
RETRY_DELAY = float(os.environ.setdefault("BENCH_RETRY_DELAY", "0"))

broker = AsyncpgBroker(
    dsn=DSN,
    channel_name=CHANNEL,
    table_name=TABLE,
    message_ttl=365 * 86400,
    connection_kwargs={"server_settings": {"application_name": "bench_worker"}},
    pool_kwargs={"server_settings": {"application_name": "bench_worker"}},
)

if FAIL_RATE:
    broker = broker.with_middlewares(
        OrderedRetryMiddleware(
            default_retry_count=MAX_RETRIES, default_delay=RETRY_DELAY
        )
    )


@broker.task(task_name="bench_task", retry_on_error=True, max_retries=MAX_RETRIES)
async def bench_task() -> int:
    """Mixed workload: ~50% CPU busy-loop, ~50% async sleep."""
    if FAIL_RATE and random.random() < FAIL_RATE:
        msg = "bench-induced failure"
        raise RuntimeError(msg)
    if random.random() < 0.5:
        deadline = time.perf_counter() + BUSY_SECONDS
        total = 0
        while time.perf_counter() < deadline:
            total += 1
        return total
    await asyncio.sleep(SLEEP_SECONDS)
    return 0


# --- Driver -------------------------------------------------------------------


def _repo_root() -> str:
    from pathlib import Path

    return str(Path(__file__).resolve().parents[1])


async def _count_rows() -> int:
    # Outstanding work = not yet terminal. This broker soft-completes (marks
    # status='completed' for TTL) instead of deleting; 'dead' is terminal too.
    value = await broker.write_pool.fetchval(
        f"SELECT count(*) FROM {TABLE} WHERE status NOT IN ('completed', 'dead')"
    )
    return int(value or 0)


async def _attempt_stats(after_id: int) -> tuple[int, int]:
    """Attempts executed and messages that burned their budget, sentinel excluded."""
    row = await broker.write_pool.fetchrow(
        f"SELECT coalesce(sum(retry_count), 0) AS attempts, "
        f"count(*) FILTER (WHERE status = 'dead') AS dead "
        f"FROM {TABLE} WHERE id > $1",
        after_id,
    )
    return (int(row["attempts"]), int(row["dead"])) if row else (0, 0)


async def _stuck_breakdown() -> str:
    """Why the queue is not draining: state of what is left, and of the oldest row."""
    # 'dead' included on purpose: one dead row halts its whole ordered group.
    rows = await broker.write_pool.fetch(
        f"SELECT status, count(*) AS n, min(scheduled_at - NOW()) AS soonest "
        f"FROM {TABLE} GROUP BY status"
    )
    states = ", ".join(f"{r['status']}={r['n']} (due in {r['soonest']})" for r in rows)
    head = await broker.write_pool.fetchrow(
        f"SELECT id, group_key, status, retry_count, ordered FROM {TABLE} "
        f"WHERE status NOT IN ('completed', 'dead') ORDER BY id LIMIT 1"
    )
    return f"{states}; head={dict(head.items()) if head else None}"


async def _remaining_ids() -> list[int]:
    # Only queued rows can be nudged with NOTIFY; active ones are in flight.
    rows = await broker.write_pool.fetch(
        f"SELECT id FROM {TABLE} WHERE status = 'queued'"
    )
    return [int(r["id"]) for r in rows]


async def _notify(ids: list[int]) -> None:
    async with broker.write_pool.acquire() as conn:
        for message_id in ids:
            await conn.execute(f"NOTIFY {CHANNEL}, '{message_id}'")


async def _spawn_worker(
    workers: int, max_async_tasks: int, show_output: bool
) -> asyncio.subprocess.Process:
    root = _repo_root()
    env = os.environ.copy()
    env["PYTHONPATH"] = root + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [
        "uv",
        "run",
        "taskiq",
        "worker",
        "bench.throughput:broker",
        "--workers",
        str(workers),
        "--max-async-tasks",
        str(max_async_tasks),
        "--ack-type",
        "when_executed",
        "--log-level",
        "INFO" if show_output else "ERROR",
    ]
    pipe = None if show_output else asyncio.subprocess.DEVNULL
    return await asyncio.create_subprocess_exec(
        *cmd,
        cwd=root,
        env=env,
        stdout=pipe,
        stderr=pipe,
        start_new_session=True,  # own process group for clean teardown
    )


async def _wait_ready(timeout: float, max_retries: int) -> bool:
    """Drain a single sentinel task to confirm the full path works.

    Re-NOTIFYs periodically in case the worker is not LISTENing yet; on this
    broker a missed NOTIFY is lost forever.
    """
    await bench_task.kicker().with_labels(max_retries=max_retries).kiq()
    sentinel_id = await broker.write_pool.fetchval(
        f"SELECT id FROM {TABLE} ORDER BY id DESC LIMIT 1"
    )
    if sentinel_id is None:
        # The sentinel was processed before we could read its id; if the table
        # has drained the worker is functioning, so treat this as success.
        return await _count_rows() == 0

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await _count_rows() == 0:
            return True
        await _notify([int(sentinel_id)])
        await asyncio.sleep(0.5)
    return await _count_rows() == 0


async def _kick_many(
    producer: AsyncpgBroker,
    offset: int,
    count: int,
    concurrency: int,
    groups: int,
    ordered: bool,
    max_retries: int,
) -> float:
    sem = asyncio.Semaphore(concurrency)

    async def one(index: int) -> None:
        async with sem:
            # Set here, not from the decorator: the driver imported this module
            # before --max-retries was parsed, so its task labels are stale.
            kicker = (
                bench_task.kicker()
                .with_broker(producer)
                .with_labels(max_retries=max_retries)
            )
            if groups:
                kicker = kicker.with_labels(
                    group_key=f"g{index % groups}", ordered=ordered
                )
            await kicker.kiq()

    start = time.monotonic()
    await asyncio.gather(*(one(offset + i) for i in range(count)))
    return time.monotonic() - start


async def _count_done(after_id: int) -> int:
    value = await broker.write_pool.fetchval(
        f"SELECT count(*) FROM {TABLE} "
        f"WHERE id > $1 AND status IN ('completed', 'dead')",
        after_id,
    )
    return int(value or 0)


async def _produce(producers: list[AsyncpgBroker], args: argparse.Namespace) -> int:
    """Kick ``--rate`` tasks per second from every producer for ``--duration``."""
    next_index = 0

    async def run(producer: AsyncpgBroker) -> int:
        nonlocal next_index
        kicked = 0
        end = time.monotonic() + args.duration
        while time.monotonic() < end:
            tick = time.monotonic()
            offset, next_index = next_index, next_index + args.rate
            await _kick_many(
                producer,
                offset,
                args.rate,
                args.kick_concurrency,
                args.groups,
                args.ordered,
                args.max_retries,
            )
            kicked += args.rate
            await asyncio.sleep(max(0.0, 1.0 - (time.monotonic() - tick)))
        return kicked

    return sum(await asyncio.gather(*(run(p) for p in producers)))


class _StallWatch:
    """Track the longest time work was outstanding while nothing completed."""

    def __init__(self, after_id: int, stall_timeout: float) -> None:
        self.after_id = after_id
        self.stall_timeout = stall_timeout
        self.longest = 0.0
        self.stalls = 0

    async def run(self) -> None:
        last_done = await _count_done(self.after_id)
        last_progress = time.monotonic()
        stalled = False
        while True:
            await asyncio.sleep(0.1)
            done = await _count_done(self.after_id)
            now = time.monotonic()
            idle = await _count_rows() == 0
            if not idle:
                self.longest = max(self.longest, now - last_progress)
            if done > last_done or idle:
                last_done, last_progress, stalled = done, now, False
                continue
            if not stalled and now - last_progress > self.stall_timeout:
                stalled = True
                self.stalls += 1
                print(
                    f"  stall: no completion for {self.stall_timeout:.0f}s; "
                    f"{await _stuck_breakdown()}",
                    file=sys.stderr,
                )


async def _drain(drain_timeout: float, stall_timeout: float) -> bool:
    """Poll until the table is empty; return whether stalled rows needed a re-NOTIFY.

    Re-NOTIFYs remaining rows if no progress is made within ``stall_timeout``.
    """
    start = time.monotonic()
    deadline = start + drain_timeout
    last_count = await _count_rows()
    last_progress = start
    recovered = False

    while True:
        remaining = await _count_rows()
        if remaining == 0:
            if recovered:
                print(
                    "  WARNING: had to re-NOTIFY stalled rows; numbers are unreliable",
                    file=sys.stderr,
                )
            return recovered

        now = time.monotonic()
        if remaining < last_count:
            last_count = remaining
            last_progress = now
        elif now - last_progress > stall_timeout:
            ids = await _remaining_ids()
            print(f"  stall detected ({remaining} rows); re-notifying", file=sys.stderr)
            await _notify(ids)
            recovered = True
            last_progress = now

        if now > deadline:
            raise TimeoutError(
                f"drain timed out with {remaining} rows remaining after "
                f"{drain_timeout:.0f}s; {await _stuck_breakdown()}"
            )
        await asyncio.sleep(0.1)


async def _teardown_worker(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=10)
    except asyncio.TimeoutError:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        await proc.wait()


def _print_report(
    args: argparse.Namespace,
    tasks: int,
    total_elapsed: float,
    attempts: int,
    dead: int,
    watch: _StallWatch,
) -> None:
    if args.groups:
        mode = f"{args.groups} groups, {'ordered' if args.ordered else 'mutex only'}"
    else:
        mode = "ungrouped"
    print("\n=== throughput report ===")
    print(f"tasks                : {tasks}")
    print(f"producers            : {args.producers}")
    if args.duration:
        print(f"duration             : {args.duration:.0f}s at {args.rate}/s each")
    print(f"workers              : {args.workers}")
    print(f"max_async_tasks      : {args.max_async_tasks}")
    print(f"mode                 : {mode}")
    if args.fail_rate:
        print(
            f"fail rate            : {args.fail_rate} "
            f"(max_retries {args.max_retries}, delay {args.retry_delay}s)"
        )
    print(f"table                : {TABLE}")
    print("-")
    print(
        f"end-to-end           : {total_elapsed:8.3f}s  "
        f"({tasks / total_elapsed:10.1f} tasks/s)"
    )
    print(
        f"longest stall        : {watch.longest:8.3f}s  "
        f"({watch.stalls} over {args.stall_timeout:.0f}s)"
    )
    if args.fail_rate:
        print(
            f"attempts executed    : {attempts:8d}  "
            f"({attempts / total_elapsed:10.1f} attempts/s)"
        )
        print(f"retries              : {attempts - tasks:8d}  ({dead} dead-lettered)")
    if args.groups:
        print(
            f"\nNOTE: --groups {args.groups} caps concurrency at {args.groups} "
            "(one active message per group)."
        )
    if args.workers > 1:
        print(
            f"\nNOTE: --workers {args.workers} compete for each message via "
            "FOR UPDATE SKIP LOCKED; every task runs exactly once."
        )


async def _run(args: argparse.Namespace) -> int:
    os.environ["BENCH_BUSY_SECONDS"] = str(args.busy_seconds)
    os.environ["BENCH_SLEEP_SECONDS"] = str(args.sleep_seconds)
    os.environ["BENCH_FAIL_RATE"] = str(args.fail_rate)
    os.environ["BENCH_MAX_RETRIES"] = str(args.max_retries)
    os.environ["BENCH_RETRY_DELAY"] = str(args.retry_delay)

    await broker.startup()
    producers = [
        AsyncpgBroker(
            dsn=DSN,
            channel_name=CHANNEL,
            table_name=TABLE,
            ensure_schema=False,
            pool_kwargs={"server_settings": {"application_name": "bench_producer"}},
        )
        for _ in range(args.producers)
    ]
    proc = None
    watcher = None
    try:
        for producer in producers:
            await producer.startup()
        proc = await _spawn_worker(
            args.workers, args.max_async_tasks, args.worker_output
        )
        print(f"spawned worker (pid={proc.pid}); waiting for readiness...")
        if not await _wait_ready(args.ready_timeout, args.max_retries):
            print("worker did not become ready in time", file=sys.stderr)
            return 1
        print("worker ready; enqueuing tasks...")

        # Everything up to here is readiness traffic; stats count only what follows.
        last_setup_id = int(
            await broker.write_pool.fetchval(
                f"SELECT coalesce(max(id), 0) FROM {TABLE}"
            )
        )
        watch = _StallWatch(last_setup_id, args.stall_timeout)
        watcher = asyncio.create_task(watch.run())
        kick_start = time.monotonic()
        if args.duration:
            tasks = await _produce(producers, args)
        else:
            shares = [args.count // len(producers)] * len(producers)
            shares[0] += args.count % len(producers)
            offsets = [sum(shares[:i]) for i in range(len(shares))]
            await asyncio.gather(
                *(
                    _kick_many(
                        p,
                        offset,
                        n,
                        args.kick_concurrency,
                        args.groups,
                        args.ordered,
                        args.max_retries,
                    )
                    for p, offset, n in zip(producers, offsets, shares)
                )
            )
            tasks = args.count
        recovered = await _drain(args.drain_timeout, args.stall_timeout)
        total_elapsed = time.monotonic() - kick_start

        attempts, dead = await _attempt_stats(last_setup_id)
        _print_report(args, tasks, total_elapsed, attempts, dead, watch)
        return 1 if watch.stalls or recovered else 0
    finally:
        async with contextlib.AsyncExitStack() as stack:
            stack.push_async_callback(broker.shutdown)
            stack.push_async_callback(_drop_bench_table)
            if proc is not None:
                stack.push_async_callback(_teardown_worker, proc)
            for producer in producers:
                stack.push_async_callback(producer.shutdown)
            if watcher is not None:
                stack.push_async_callback(_stop_watcher, watcher)


async def _stop_watcher(watcher: asyncio.Task[None]) -> None:
    watcher.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher


async def _drop_bench_table() -> None:
    await broker.write_pool.execute(f"DROP TABLE IF EXISTS {TABLE}")
    await broker.write_pool.execute(
        f"DROP FUNCTION IF EXISTS {broker.claim_fn}(BIGINT)"
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="End-to-end throughput stress test for the taskiq-pg broker "
        "(uses a real taskiq worker). DSN/table/channel are read from BENCH_* "
        "env vars; see module docstring."
    )
    parser.add_argument("-n", "--count", type=int, default=5000)
    parser.add_argument(
        "-p",
        "--producers",
        type=int,
        default=1,
        help="producer brokers kicking concurrently, each with its own pool",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="kick for this many seconds at --rate per producer, instead of -n",
    )
    parser.add_argument(
        "--rate", type=int, default=100, help="tasks/s per producer with --duration"
    )
    parser.add_argument("-w", "--workers", type=int, default=1)
    parser.add_argument("-m", "--max-async-tasks", type=int, default=100)
    parser.add_argument("--busy-seconds", type=float, default=BUSY_SECONDS)
    parser.add_argument("--sleep-seconds", type=float, default=SLEEP_SECONDS)
    parser.add_argument("--kick-concurrency", type=int, default=50)
    parser.add_argument("--ready-timeout", type=float, default=30.0)
    parser.add_argument("--drain-timeout", type=float, default=300.0)
    parser.add_argument("--stall-timeout", type=float, default=10.0)
    parser.add_argument("--worker-output", action="store_true")
    parser.add_argument(
        "--groups",
        type=int,
        default=0,
        help="spread tasks over N group_keys (0 = ungrouped)",
    )
    parser.add_argument(
        "--ordered",
        action="store_true",
        help="FIFO within each group; requires --groups",
    )
    parser.add_argument(
        "--fail-rate",
        type=float,
        default=0.0,
        help="per-attempt failure probability (0 = no retries)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RETRIES,
        help="attempt budget per message",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=RETRY_DELAY,
        help="backoff seconds on retry",
    )
    args = parser.parse_args()
    if args.ordered and not args.groups:
        parser.error("--ordered requires --groups")
    if args.producers < 1:
        parser.error("--producers must be at least 1")
    if args.duration < 0:
        parser.error("--duration must not be negative")
    if args.duration and args.rate < 1:
        parser.error("--rate must be at least 1 with --duration")
    return args


def main() -> None:
    raise SystemExit(asyncio.run(_run(_parse_args())))


if __name__ == "__main__":
    main()
