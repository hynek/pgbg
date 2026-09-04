from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol

import psycopg


if TYPE_CHECKING:
    from ._dispatcher import Subscription


type ConnectionProvider = Callable[
    [], AbstractContextManager[psycopg.Connection[Any]]
]
"""
Provides short-lived connections for elections and renewals. It's called
once per database visit, so try to utilize some kind of connection pooling.

Connections must be lent out with **no** transaction in progress. The lease
SQL assumes [`READ COMMITTED`][rc] semantics.

[rc]: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED
"""


class Subscribable(Protocol):
    """
    Anything that hands out channel subscriptions.

    Both `NotifyDispatcher` and `SupervisedDispatcher` satisfy it, so code that
    only needs to subscribe (like a background service wiring up its wakeup)
    can take either.
    """

    def subscribe(self, channel: str) -> Subscription:
        """
        Subscribe to *channel*.
        """
        ...
