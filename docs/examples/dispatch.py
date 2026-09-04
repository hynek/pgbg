import argparse
import time

import structlog

from bgt import as_work_factory
from sqlalchemy import Engine, create_engine

from pgbg.sqlalchemy import start_dispatcher, start_elected_service


logger = structlog.get_logger()


def do_foo() -> bool:
    logger.info("foo!")
    return False


def do_bar() -> bool:
    logger.info("bar!")
    return False


def main(engine: Engine, worker_id: str) -> None:
    with start_dispatcher(engine) as dispatcher:
        logger.info("dispatcher started")

        foo_sub = dispatcher.subscribe("foo")
        bar_sub = dispatcher.subscribe("bar")

        with (
            start_elected_service(
                as_work_factory(do_foo),
                engine,
                wakeup=foo_sub,
                name="foo-worker",
                worker_id=worker_id,
                interval=6000,  # (1)!
            ),
            start_elected_service(
                as_work_factory(do_bar),
                engine,
                wakeup=bar_sub,
                name="bar-worker",
                worker_id=worker_id,
                interval=6000,
            ),
        ):
            try:
                while True:
                    time.sleep(100)
            except KeyboardInterrupt:
                logger.info("dispatcher stopping")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run an example elected dispatcher"
    )
    parser.add_argument(
        "worker_id", metavar="WORKER-ID", help="Unique worker identifier"
    )
    parser.add_argument(
        "database_url",
        nargs="?",
        default="postgresql+psycopg://pgbg@127.0.0.1/pgbg",
        help="Database connection URL",
    )
    args = parser.parse_args()

    engine = create_engine(args.database_url)
    try:
        main(engine, args.worker_id)
    finally:
        engine.dispose()
