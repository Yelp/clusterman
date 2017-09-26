import pytest

from clusterman.math.piecewise import PiecewiseConstantFunction


@pytest.fixture
def fn():
    return PiecewiseConstantFunction(1)
