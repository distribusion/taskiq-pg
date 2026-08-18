# ruff: noqa: T201, S608, S101
"""Race drills. Probabilistic by construction -- not tests, do not put in the suite.

Both failures need one worker to commit mid-statement of another's dequeue; that
interleaving cannot be driven by hand, so these hammer until it happens and report a
hit rate. A clean run proves nothing on its own.

  mutex      -- two rows of one group 'active' at once. Checks committed state on
                every claim, not a sampled counter.
  orphan     -- a row marked 'active' that no consumer received. Needs inserts to
                land while claims are in flight, so it produces and consumes at once.

Usage: uv run python -m bench.races [mutex|orphan|all] [rounds]
"""

from __future__ import annotations

import asyncio
import os
import random
import string
import sys
import uuid

import asyncpg
from taskiq import BrokerMessage

from taskiq_pg.broker import AsyncpgBroker
from taskiq_pg.broker_queries import COMPLETE_MESSAGE_QUERY

DSN = os.environ.get(
    "POSTGRESQL_URL", "postgresql://postgres:postgres@localhost:25432/postgres"
)


async def _broker() -> AsyncpgBroker:
    table = "drill_" + "".join(random.choice(string.ascii_lowercase) for _ in range(8))
    broker = AsyncpgBroker(
        dsn=DSN,
        channel_name=f"{table}_ch",
        table_name=table,
        job_lock_keyspace=random.randint(1000, 9999),
        enable_sweeping=False,
    )
    await broker.startup()
    return broker


async def _drop(broker: AsyncpgBroker) -> None:
    assert broker.write_pool is not None
    await broker.write_pool.execute(f"DROP TABLE IF EXISTS {broker.table_name}")
    await broker.shutdown()


async def mutex_drill(groups: int = 2, backlog: int = 800, workers: int = 8) -> int:
    """Deep backlog so each dequeue spans a rival's commit. Returns violation count."""
    broker = await _broker()
    table = broker.table_name
    try:
        for i in range(backlog):
            await broker.kick(
                BrokerMessage(
                    task_id=uuid.uuid4().hex,
                    task_name="t",
                    message=b"x",
                    labels={"group_key": f"g{i % groups}"},
                )
            )
        complete_sql = COMPLETE_MESSAGE_QUERY.format(table_name=table)
        violations: list[tuple[str, int]] = []
        claimed: list[int] = []

        async def worker() -> None:
            conn = await asyncpg.connect(DSN)
            try:
                while True:
                    async with conn.transaction():
                        row = await broker._claim_on(conn)
                    if row is None:
                        left = await conn.fetchval(
                            f"SELECT COUNT(*) FROM {table} WHERE status = 'queued'"
                        )
                        if left == 0:
                            return
                        await asyncio.sleep(0.005)
                        continue
                    claimed.append(int(row["id"]))
                    active = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {table} "
                        "WHERE group_key = $1 AND status = 'active'",
                        row["group_key"],
                    )
                    if int(active) > 1:
                        violations.append((str(row["group_key"]), int(active)))
                    await asyncio.sleep(0.005)
                    await conn.execute(complete_sql, broker.message_ttl, row["id"])
            finally:
                await conn.close()

        await asyncio.gather(*(worker() for _ in range(workers)))
        assert len(claimed) == backlog, f"lost rows: {len(claimed)} of {backlog}"
        assert len(set(claimed)) == backlog, "a row was claimed twice"
        return len(violations)
    finally:
        await _drop(broker)


async def orphan_drill(
    total: int = 2000, producers: int = 8, consumers: int = 12
) -> int:
    """Produce while claiming. Returns count of active rows nobody received."""
    broker = await _broker()
    table = broker.table_name
    try:
        complete_sql = COMPLETE_MESSAGE_QUERY.format(table_name=table)
        delivered: list[int] = []
        producing = asyncio.Event()

        async def produce(worker: int) -> None:
            for i in range(total // producers):
                index = worker * total + i
                labels = {} if index % 3 == 0 else {"group_key": f"g{index % 20}"}
                await broker.kick(
                    BrokerMessage(
                        task_id=uuid.uuid4().hex,
                        task_name="t",
                        message=b"x",
                        labels=labels,
                    )
                )

        async def produce_all() -> None:
            try:
                await asyncio.gather(*(produce(w) for w in range(producers)))
            finally:
                producing.set()

        async def consume() -> None:
            # quiet period, not "queue empty": an orphan blocks its group forever
            conn = await asyncpg.connect(DSN)
            idle = 0
            try:
                while idle < 40:
                    async with conn.transaction():
                        row = await broker._claim_on(conn)
                    if row is None:
                        idle = idle + 1 if producing.is_set() else 0
                        await asyncio.sleep(0.005)
                        continue
                    idle = 0
                    delivered.append(int(row["id"]))
                    await conn.execute(complete_sql, broker.message_ttl, row["id"])
            finally:
                await conn.close()

        await asyncio.wait_for(
            asyncio.gather(produce_all(), *(consume() for _ in range(consumers))),
            timeout=120,
        )
        assert broker.write_pool is not None
        orphaned = await broker.write_pool.fetchval(
            f"SELECT COUNT(*) FROM {table} WHERE status = 'active'"
        )
        assert len(delivered) == total, f"lost rows: {len(delivered)} of {total}"
        return int(orphaned)
    finally:
        await _drop(broker)


async def main() -> int:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    drills = {"mutex": mutex_drill, "orphan": orphan_drill}
    if which != "all":
        drills = {which: drills[which]}

    failed = 0
    for name, drill in drills.items():
        hits = 0
        for attempt in range(1, rounds + 1):
            found = await drill()
            hits += bool(found)
            print(f"{name} run {attempt}: {found}")
        print(f"{name}: detected in {hits}/{rounds} rounds\n")
        failed += hits
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
