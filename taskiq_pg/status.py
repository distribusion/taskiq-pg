"""Message lifecycle states."""

from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:  # 3.9/3.10 shim; StrEnum landed in 3.11
    from enum import Enum

    class StrEnum(str, Enum):
        """str-valued Enum backport."""

        def __str__(self) -> str:
            return str(self.value)


class MessageStatus(StrEnum):
    """Lifecycle states of a broker message."""

    QUEUED = "queued"
    ACTIVE = "active"
    COMPLETED = "completed"
    DEAD = "dead"  # exhausted max_retry_attempts; parked for inspection
