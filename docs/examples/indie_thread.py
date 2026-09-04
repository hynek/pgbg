import random
import time

import bgt
import structlog


logger = structlog.get_logger()


def _toss_coin() -> bool:
    return random.choice([True, False])


def do_work() -> bool:
    if _toss_coin() and _toss_coin():
        raise RuntimeError("oh no I've crashed!")

    logger.info("did some work!")

    return _toss_coin()


def main() -> None:
    svc = bgt.SupervisedService.start(
        bgt.as_work_factory(do_work),
        name="example-thread",
        wakeup=bgt.IntervalOnlyWakeup(),  # only wake up on intervals
        interval=2,  # which are 2 seconds long
    )

    try:
        time.sleep(10)
    except KeyboardInterrupt:
        logger.info("shutting down")

    svc.stop()


if __name__ == "__main__":
    main()
