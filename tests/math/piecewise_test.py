from datetime import timedelta

import arrow
import pytest

from clusterman.math.piecewise import hour_transform
from clusterman.math.piecewise import PiecewiseConstantFunction


def sorteddict_values_assert(sdict, values):
    assert list(sdict.values()) == values


@pytest.fixture
def fn():
    return PiecewiseConstantFunction(1)


def test_construct_function(fn):
    fn.modify_value(2, 2)
    fn.modify_value(3, 1)
    fn.modify_value(1, -3)

    assert fn.call(0) == 1
    assert fn.call(1) == -2
    assert fn.call(2) == 0
    assert fn.call(3) == 1
    assert fn.call(4) == 1


def test_integrals_no_points(fn):
    sorteddict_values_assert(fn.integrals(0, 10.5, 1), [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0.5])


def test_integrals_whole_range(fn):
    sorteddict_values_assert(fn.integrals(0, 2, 2), [2])


def test_integrals_one_point(fn):
    fn = PiecewiseConstantFunction(1)
    fn.modify_value(2, 2)
    sorteddict_values_assert(fn.integrals(0, 10.5, 1), [1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 1.5])
    sorteddict_values_assert(fn.integrals(0.5, 11, 1), [1, 2, 3, 3, 3, 3, 3, 3, 3, 3, 1.5])


def test_integrals_two_point(fn):
    fn.modify_value(2, 2)
    fn.modify_value(10.25, -2)
    sorteddict_values_assert(fn.integrals(0, 10.5, 1), [1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 1])
    sorteddict_values_assert(fn.integrals(0.5, 11, 1), [1, 2, 3, 3, 3, 3, 3, 3, 3, 2.5, 0.5])


def test_integrals_multi_points(fn):
    fn.modify_value(1.5, 2)
    fn.modify_value(2, -1)
    fn.modify_value(3, -1)
    fn.modify_value(4, 2)
    fn.modify_value(7, -1)
    sorteddict_values_assert(fn.integrals(0, 10.5, 3), [5, 7, 7, 3])


def test_integrals_with_timedeltas(fn):
    for i in range(10):
        fn.modify_value(arrow.get(i * 60), 1)
    x = fn.integrals(arrow.get(0), arrow.get(10 * 60), timedelta(seconds=60), transform=hour_transform)
    sorteddict_values_assert(x, pytest.approx([60 / 3600 * i for i in range(2, 12)]))


@pytest.mark.parametrize('initial_value', (0, 1))
def test_constant_integral(initial_value):
    fn = PiecewiseConstantFunction(initial_value)
    assert fn.integral(0, 1) == initial_value


@pytest.mark.parametrize('xval,result', ((0, 2), (1, 1)))
def test_one_step_integral(xval, result):
    fn = PiecewiseConstantFunction()
    fn.modify_value(xval, 1)
    assert fn.integral(0, 2) == result
    assert fn.integral(3, 4) == 1
    assert fn.integral(-2, -1) == 0


def test_one_step_integral_two_changes():
    fn = PiecewiseConstantFunction()
    fn.modify_value(1, 10)
    fn.modify_value(1, -5)
    assert fn.integral(0, 2) == 5


def test_multistep_integral(fn):
    fn.modify_value(1, 19)
    fn.modify_value(2, -10)
    fn.modify_value(2, 5)
    fn.modify_value(3, -10)
    sorteddict_values_assert(fn.integrals(0, 4, 4), [41])
    assert fn.integral(0, 4) == 41
    assert fn.integral(2, 3) == 15
