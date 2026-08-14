import random
import time

from collections.abc import Generator
from contextlib import contextmanager

import structlog

import pgbg

from pgbg.typing import DoWork


logger = structlog.get_logger()


def _toss_coin() -> bool:
    return random.choice([True, False])


def do_work() -> bool:
    if _toss_coin() and _toss_coin():
        raise RuntimeError("oh no I've crashed!")

    logger.info("did some work!")

    return _toss_coin()


@contextmanager
def make_work() -> Generator[DoWork]:
    logger.info("work init!")
    try:
        yield do_work
    finally:
        logger.info("work cleanup!")


def main() -> None:
    svc = pgbg.SupervisedService.start(
        make_work,
        name="example-thread",
        wakeup=pgbg.IntervalOnlyWakeup(),  # only wake up on intervals
        interval=2,  # which are 2 seconds
    )

    try:
        time.sleep(10)
    except KeyboardInterrupt:
        logger.info("shutting down")

    svc.stop()


if __name__ == "__main__":
    main()
