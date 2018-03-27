import math
from functools import lru_cache
from heapq import merge
from itertools import zip_longest

from sortedcontainers import SortedDict


_LRU_CACHE_SIZE = 5


def hour_transform(td):
    return td.total_seconds() / 3600


def piecewise_breakpoint_generator(breakpoints, start_time, end_time):
    for x in breakpoints.irange(start_time, end_time):
        yield x
    yield end_time


class PiecewiseConstantFunction:

    def __init__(self, initial_value=0):
        """ Initialize the constant function to a particular value

        :param initial_value: the starting value for the function
        """
        self.breakpoints = SortedDict()
        self._initial_value = initial_value

    def add_breakpoint(self, xval, yval, squash=True):
        """ Add a breakpoint to the function and update the value

        Let f(x) be the original function, and next_bp be the first breakpoint > xval; after calling
        this method, the function will be modified to f'(x) = yval for x \in [xval, next_bp)

        :param xval: the x-position of the breakpoint to add/modify
        :param yval: the value to set the function to at xval
        :param squash: if True and f(xval) = yval before calling this method, the function will remain unchanged
        """
        if squash and self.call(xval) == yval:
            return
        self.breakpoints[xval] = yval

    def add_delta(self, xval, delta):
        """ Modify the function value for x >= xval

        Let f(x) be the original function; After calling this method,
        the function will be modified to f'(x) = f(x) + delta for all x >= xval

        :param xval: the x-position of the breakpoint to add/modify
        :param delta: the amount to shift the function value by at xval
        """
        if delta == 0:
            return

        if xval not in self.breakpoints:
            self.breakpoints[xval] = self.call(xval)

        for x in self.breakpoints.irange(xval):
            self.breakpoints[x] += delta

        self.values.cache_clear()
        self.integrals.cache_clear()

    def call(self, xval):
        """ Compute the output of the function at a point

        :param xval: the x-position to compute
        :returns: f(xval)
        """
        if len(self.breakpoints) == 0 or xval < self.breakpoints.keys()[0]:
            return self._initial_value
        else:
            lower_index = self.breakpoints.bisect(xval) - 1
            return self.breakpoints.values()[lower_index]

    def _breakpoint_info(self, index):
        """ Helper function for computing breakpoint information

        :param index: index of the breakpoint to compute
        :returns: (index, breakpoint, value)
          * index is the breakpoint index (if it exists), or None if we're off the end
          * breakpoint is the x-value of the breakpoint, or None if we're off the end
          * value is f(breakpoint), or f(last_breakpoint) if we're off the end
        """
        try:
            breakpoint, value = self.breakpoints.peekitem(index)
        except IndexError:
            index = None
            breakpoint, value = None, self.breakpoints.values()[-1]
        return (index, breakpoint, value)

    @lru_cache(maxsize=_LRU_CACHE_SIZE)  # cache results of calls to this function
    def values(self, start, stop, step):
        """ Compute a sequence of values of the function

        This is more efficient than [self.call(xval) for xval in range(start, stop, step)] because each self.call(..)
        takes O(log n) time due to the binary tree structure of self._breakpoints.  This method can compute the range
        of values in linear time in the range, which is significantly faster for large value ranges.

        :param start: lower bound of value sequence
        :param stop: upper bound of value sequence
        :param step: width between points in the sequence
        :returns: a SortedDict of the values of the function between start and stop, with the x-distance between
            each data-point equal to `step`; like normal "range" functions the right endpoint is not included
        """

        step = step or (stop - start)
        if len(self.breakpoints) == 0:
            num_values = int(math.ceil((stop - start) / step))
            return SortedDict([(start + step * i, self._initial_value) for i in range(num_values)])

        curr_xval = start
        curr_value = self.call(start)
        next_index, next_breakpoint, next_value = self._breakpoint_info(self.breakpoints.bisect(start))

        sequence = SortedDict()
        while curr_xval < stop:
            sequence[curr_xval] = curr_value

            next_xval = min(stop, curr_xval + step)
            while next_breakpoint and next_xval >= next_breakpoint:
                curr_value = next_value
                next_index, next_breakpoint, next_value = self._breakpoint_info(next_index + 1)
            curr_xval = next_xval

        return sequence

    @lru_cache(maxsize=_LRU_CACHE_SIZE)  # cache results of calls to this function
    def integrals(self, start, stop, step, transform=lambda x: x):
        """ Compute a sequence of integrals of the function

        :param start: lower bound of integral sequence
        :param stop: upper bound of integral sequence
        :param step: width of each "chunk" of the integral sequence
        :param transform: function to apply to x-widths before computing the integral
        :returns: a SortedDict of the numeric integral values of the function between start and stop;
            each integral has a range of size `step`, and the key-value is the left endpoint of the chunk
        """
        step = step or (stop - start)
        if len(self.breakpoints) == 0:
            # If there are no breakpoints, just split up the range into even widths and compute
            # (width * self._initial_value) for each chunk.
            step_width = transform(step)
            range_width = transform(stop - start)
            num_full_chunks = int(range_width // step_width)
            sequence = SortedDict([
                (start + step * i, step_width * self._initial_value)
                for i in range(num_full_chunks)
            ])

            # If the width does not evenly divide the range, compute the last chunk separately
            if range_width % step_width != 0:
                sequence[start + step * num_full_chunks] = range_width % step_width * self._initial_value
            return sequence

        # Set up starting loop parameters
        curr_xval = start
        curr_value = self.call(start)
        next_index, next_breakpoint, next_value = self._breakpoint_info(self.breakpoints.bisect(start))

        # Loop through the entire range and compute the integral of each chunk
        sequence = SortedDict()
        while curr_xval < stop:
            orig_xval = curr_xval
            next_xval = min(stop, curr_xval + step)

            # For each breakpoint in [curr_xval, next_xval), compute the area of that sub-chunk
            next_integral = 0
            while next_breakpoint and next_xval >= next_breakpoint:
                next_integral += transform(next_breakpoint - curr_xval) * curr_value
                curr_xval = next_breakpoint
                curr_value = next_value
                next_index, next_breakpoint, next_value = self._breakpoint_info(next_index + 1)

            # Handle any remaining width between the last breakpoint and the end of the chunk
            next_integral += transform(next_xval - curr_xval) * curr_value
            sequence[orig_xval] = next_integral

            curr_xval = next_xval

        return sequence

    def integral(self, start, stop, transform=lambda x: x):
        """ Helper function to compute the integral of the whole specified range

        :param start: lower bound of the integral
        :param stop: upper bound of the integral
        :returns: the integral of the function between start and stop
        """
        return self.integrals(start, stop, (stop - start), transform).values()[0]

    def __str__(self):
        ret = f'{self._initial_value}, x < {self.breakpoints.keys()[0]}\n'
        for xval, yval in self.breakpoints.items():
            ret += f'{yval}, x >= {xval}\n'
        return ret

    def __add__(self, other):
        new_func = PiecewiseConstantFunction(self._initial_value + other._initial_value)
        for xval, y0, y1 in _merged_breakpoints(self, other):
            new_func.add_breakpoint(xval, y0 + y1)
        return new_func

    def __sub__(self, other):
        new_func = PiecewiseConstantFunction(self._initial_value - other._initial_value)
        for xval, y0, y1 in _merged_breakpoints(self, other):
            new_func.add_breakpoint(xval, y0 - y1)
        return new_func

    def __mul__(self, other):
        new_func = PiecewiseConstantFunction(self._initial_value * other._initial_value)
        for xval, y0, y1 in _merged_breakpoints(self, other):
            new_func.add_breakpoint(xval, y0 * y1)
        return new_func

    def __truediv__(self, other):
        try:
            new_func = PiecewiseConstantFunction(self._initial_value / other._initial_value)
        except ZeroDivisionError:
            new_func = PiecewiseConstantFunction()

        for xval, y0, y1 in _merged_breakpoints(self, other):
            try:
                new_func.add_breakpoint(xval, y0 / y1)
            except ZeroDivisionError:
                new_func.add_breakpoint(xval, 0)
        return new_func


def _merged_breakpoints(fn0, fn1):
    bp0 = zip_longest(fn0.breakpoints.items(), [], fillvalue=0)
    bp1 = zip_longest(fn1.breakpoints.items(), [], fillvalue=1)
    yprev0, yprev1 = fn0._initial_value, fn1._initial_value

    for (x, y), fnnum in merge(bp0, bp1):
        if fnnum == 0:
            yield x, y, yprev1
            yprev0 = y
        elif fnnum == 1:
            yield x, yprev0, y
            yprev1 = y
