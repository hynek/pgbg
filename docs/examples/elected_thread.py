import argparse
import time

import structlog

from sqlalchemy import Engine, create_engine

import pgbg

from pgbg.sqlalchemy import start_elected_service


logger = structlog.get_logger()


def do_work() -> bool:
    logger.info("did some work!")

    return False


def main(engine: Engine, worker_id: str) -> None:
    svc = start_elected_service(
        pgbg.as_work_factory(do_work),
        engine,
        name="example-thread",
        worker_id=worker_id,
        wakeup=pgbg.IntervalOnlyWakeup(),  # still interval-only
        interval=2,
    )

    try:
        time.sleep(100)
    except KeyboardInterrupt:
        logger.info("shutting down")

    svc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run an example elected service"
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
