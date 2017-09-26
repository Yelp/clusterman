from sortedcontainers import SortedDict


def hour_transform(td):
    return td.total_seconds() / 3600


class PiecewiseConstantFunction:
    """ Build a piecewise constant function by iteratively appending values to the trailing (right-hand) edge """

    def __init__(self, initial_value=0):
        """ Initialize the constant function to a particular value

        :param initial_value: the starting value for the function
        """
        self._breakpoints = SortedDict()
        self._initial_value = initial_value

    def modify_value(self, xval, delta):
        """ Modify the function value for x >= xval

        Let f(x) be the original function; After calling this method,
        the function will be modified to f'(x) = f(x) + delta for all x >= xval

        :param xval: the x-position of the breakpoint to add/modify
        :param delta: the amount to shift the function value by at xval
        """
        if xval not in self._breakpoints:
            self._breakpoints[xval] = self.call(xval)

        for x in self._breakpoints.irange(xval):
            self._breakpoints[x] += delta

    def call(self, xval):
        """ Compute the output of the function at a point

        :param xval: the x-position to compute
        :returns: f(xval)
        """
        if len(self._breakpoints) == 0 or xval < self._breakpoints.keys()[0]:
            return self._initial_value
        else:
            lower_index = self._breakpoints.bisect(xval) - 1
            return self._breakpoints.values()[lower_index]

    def _breakpoint_info(self, index):
        """ Helper function for computing breakpoint information

        :param index: index of the breakpoint to compute
        :returns: (index, breakpoint, value)
          * index is the breakpoint index (if it exists), or None if we're off the end
          * breakpoint is the x-value of the breakpoint, or None if we're off the end
          * value is f(breakpoint), or f(last_breakpoint) if we're off the end
        """
        try:
            breakpoint = self._breakpoints.keys()[index]
            value = self._breakpoints.values()[index]
        except IndexError:
            index = None
            breakpoint = None
            value = self._breakpoints.values()[-1]

        return (index, breakpoint, value)

    def integrals(self, start, stop, step, transform=lambda x: x):
        """ Compute a sequence of integrals of the function

        :param start: lower bound of integral sequence
        :param stop: upper bound of integral sequence
        :param step: width of each "chunk" of the integral sequence
        :param transform: function to apply to x-widths before computing the integral
        :returns: a SortedDict of the numeric integral values of the function between start and stop;
            each integral has a range of size `step`, and the key-value is the left endpoint of the chunk
        """
        if len(self._breakpoints) == 0:
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
        next_index, next_breakpoint, next_value = self._breakpoint_info(self._breakpoints.bisect(start))

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
        ret = f'{self._initial_value}, x < {self._breakpoints.keys()[0]}\n'
        for xval, yval in self._breakpoints.items():
            ret += f'{yval}, x >= {xval}\n'
        return ret
