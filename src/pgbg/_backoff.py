import math
import random

from collections.abc import Generator


def backoff_iter(start: float, stop: float) -> Generator[float]:
    """
    Yield exponentially increasing numbers from *start* until *stop*, forever.

    Every yield draws its own jitter of up to 10 percent in each direction,
    so herds of restarts spread out instead of stampeding in step. The cap
    applies to the base value, so a yield can overshoot *stop* by the jitter.
    A stop of 0 can be useful in testing.
    """
    start = float(start)
    stop = float(stop)

    if start < 0.0 or not math.isfinite(start):
        msg = f"expected start >= 0, not {start!r}"
        raise ValueError(msg)
    if stop < start or not math.isfinite(stop):
        msg = f"expected stop >= start, not {stop!r}"
        raise ValueError(msg)

    cur = start
    while True:
        yield min(cur, stop) * random.uniform(0.9, 1.1)  # noqa: S311

        if cur == 0:
            cur = 1
        elif cur < stop:
            cur = min(cur * 2, stop)
