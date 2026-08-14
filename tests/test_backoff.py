"""
Tests for `pgbg._backoff`.
"""

from unittest.mock import patch

import pytest

from pgbg._backoff import backoff_iter


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"start": -1, "stop": 30}, "expected start >= 0"),
        ({"start": 5, "stop": 4}, "expected stop >= start"),
    ],
)
def test_rejects_actually_invalid_arguments(kwargs, match):
    """
    Only actually-invalid values are rejected: a negative start and a stop
    below start.
    """
    with pytest.raises(ValueError, match=match):
        next(backoff_iter(**kwargs))


@pytest.mark.usefixtures("no_jitter")
def test_start_zero_bootstraps_to_one():
    """
    start=0 is legit: the first yield is 0.0 and the ladder bootstraps to
    1 before doubling.
    """
    gen = backoff_iter(0, 30)

    assert [0.0, 1.0, 2.0, 4.0, 8.0] == [next(gen) for _ in range(5)]


@pytest.mark.usefixtures("no_jitter")
def test_doubles_and_caps_at_stop():
    """
    Values double until *stop* and stay capped there.
    """
    gen = backoff_iter(1, 8)

    assert [1.0, 2.0, 4.0, 8.0, 8.0, 8.0] == [next(gen) for _ in range(6)]


@pytest.mark.usefixtures("no_jitter")
def test_stop_zero_yields_only_zeros():
    """
    stop=0 pins every yield to 0.0.
    """
    gen = backoff_iter(0, 0)

    assert [0.0, 0.0, 0.0] == [next(gen) for _ in range(3)]


def test_jitter_is_drawn_per_yield():
    """
    Every yield draws its own jitter from +-10 percent.
    """
    with patch("pgbg._backoff.random.uniform", side_effect=[0.9, 1.1, 1.0]):
        gen = backoff_iter(1, 30)

        # 1*0.9, 2*1.1, 4*1.0
        assert [0.9, 2.2, 4.0] == [next(gen) for _ in range(3)]


@pytest.mark.usefixtures("no_jitter")
def test_coerces_int_inputs_to_float():
    """
    Int arguments are coerced: the yields are floats.
    """
    first = next(backoff_iter(1, 8))

    assert 1.0 == first
    assert isinstance(first, float)
