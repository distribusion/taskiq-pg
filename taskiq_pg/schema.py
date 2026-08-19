"""Schema installation for the broker's table, indexes and claim function."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from taskiq_pg.broker_queries import CLAIM_FUNCTION_QUERY, CREATE_TABLE_QUERY

if TYPE_CHECKING:
    import asyncpg

_FINGERPRINT_PROBE = "SELECT obj_description(to_regclass($1), 'pg_class')"


def safe_name(table_name: str) -> str:
    """Table name usable as an identifier suffix."""
    return table_name.replace('"', "").replace(" ", "_")


def claim_fn_name(table_name: str) -> str:
    """Name of the per-table claim function. The table name is baked into its body."""
    return f"{safe_name(table_name)}_claim"


def _ddl(table_name: str) -> tuple[str, ...]:
    return (
        CREATE_TABLE_QUERY.format(
            table_name=table_name, table_name_safe=safe_name(table_name)
        ),
        CLAIM_FUNCTION_QUERY.format(
            table_name=table_name, claim_fn=claim_fn_name(table_name)
        ),
    )


async def ensure(
    conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
    table_name: str,
    keyspace: int,
) -> None:
    """Install the schema if a fingerprint says it is not current.

    Concurrent DDL from N starting workers fails: CREATE OR REPLACE FUNCTION raises
    "tuple concurrently updated", CREATE INDEX IF NOT EXISTS can raise a duplicate
    key. So run DDL only when needed and serialise that path. The second probe keeps
    a cold start to one DDL run rather than N.

    The fingerprint is stored as the table's comment, so any change to the DDL text
    reinstalls it -- unlike IF NOT EXISTS, which keeps a stale definition forever.
    """
    ddl = _ddl(table_name)
    fingerprint = hashlib.sha256("".join(ddl).encode()).hexdigest()[:32]

    if await conn.fetchval(_FINGERPRINT_PROBE, table_name) == fingerprint:
        return

    async with conn.transaction():
        _ = await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, $2))",
            f"ddl:{table_name}",
            keyspace,
        )
        if await conn.fetchval(_FINGERPRINT_PROBE, table_name) == fingerprint:
            return
        for statement in ddl:
            _ = await conn.execute(statement)
        # hex digest, safe to inline; COMMENT takes no parameters
        _ = await conn.execute(f"COMMENT ON TABLE {table_name} IS '{fingerprint}'")
