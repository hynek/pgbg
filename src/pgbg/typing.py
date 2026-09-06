from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Protocol

import psycopg

from ._dispatcher import Subscription


class ConnectionProvider(Protocol):
    """
    Provides short-lived connections for elections and renewals.

    It's called once per database visit, so try to utilize some kind of
    connection pooling.

    Connections must be lent out with **no** transaction in progress. The lease
    SQL assumes [`READ COMMITTED`][rc] semantics.

    A plain connect function satisfies it, because a Psycopg connection is its
    own context manager. So does
    a [`psycopg_pool.ConnectionPool.connection()`][psycopg_pool.ConnectionPool.connection]
    method.

    [rc]: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED
    """

    def __call__(self) -> AbstractContextManager[psycopg.Connection[Any]]:
        """
        Lend a connection.

        Returns:
            A context manager that yields a connection with no transaction
                in progress and takes it back when the block ends.
        """
        ...


class Subscribable(Protocol):
    """
    Anything that hands out channel subscriptions.

    Both [`NotifyDispatcher`][pgbg.NotifyDispatcher] and
    [`SupervisedDispatcher`][pgbg.SupervisedDispatcher] satisfy it, so code
    that only needs to subscribe (like a background service wiring up its
    wakeup) can take either.
    """

    def subscribe(self, channel: str) -> Subscription:
        """
        Subscribe to *channel*.

        Args:
            channel: The name of the channel to subscribe to.

        Returns:
            A subscription object that will receive notifications from the
                channel.
        """
        ...
