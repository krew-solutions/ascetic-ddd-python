import math
import re
import statistics
from bisect import bisect_left, bisect_right
from collections import defaultdict
from functools import cached_property


__all__ = ("Stats", "Collector",)


class Stats:
    """
    Based on source code:
    - https://github.com/ionelmc/pytest-benchmark/blob/master/src/pytest_benchmark/stats.py
    - https://github.com/psf/pyperf/blob/main/pyperf/_bench.py
    - https://github.com/psf/pyperf/blob/main/pyperf/_utils.py
    """
    fields = (
        'min',
        'max',
        'mean',
        'stddev',
        'rounds',
        'median',
        'median_abs_dev',
        'iqr',
        'q1',
        'q3',
        'iqr_outliers',
        'stddev_outliers',
        'outliers',
        'ld15iqr',
        'hd15iqr',
        'ops',
        'total',
    )

    def __init__(self, data: list | None = None):
        self.data = data or []

    def __bool__(self):
        return bool(self.data)

    def __nonzero__(self):
        return bool(self.data)

    def as_dict(self):
        return {field: getattr(self, field) for field in self.fields}

    def update(self, duration):
        self.data.append(duration)

    @cached_property
    def sorted_data(self):
        return sorted(self.data)

    @cached_property
    def total(self):
        return sum(self.data)

    @cached_property
    def min(self):
        return min(self.data)

    @cached_property
    def max(self):
        return max(self.data)

    @cached_property
    def mean(self):
        return statistics.mean(self.data)

    @cached_property
    def stddev(self):
        if len(self.data) > 1:
            return statistics.stdev(self.data)
        else:
            return 0

    @property
    def stddev_outliers(self):
        """
        Count of StdDev outliers: what's beyond (Mean - StdDev, Mean - StdDev)
        """
        count = 0
        q0 = self.mean - self.stddev
        q4 = self.mean + self.stddev
        for val in self.data:
            if val < q0 or val > q4:
                count += 1
        return count

    @cached_property
    def rounds(self):
        return len(self.data)

    @cached_property
    def median(self):
        return statistics.median(self.data)

    @cached_property
    def ld15iqr(self):
        """
        Tukey-style Lowest Datum within 1.5 IQR under Q1.
        """
        if len(self.data) == 1:
            return self.data[0]
        else:
            return self.sorted_data[bisect_left(self.sorted_data, self.q1 - 1.5 * self.iqr)]

    @cached_property
    def hd15iqr(self):
        """
        Tukey-style Highest Datum within 1.5 IQR over Q3.
        """
        if len(self.data) == 1:
            return self.data[0]
        else:
            pos = bisect_right(self.sorted_data, self.q3 + 1.5 * self.iqr)
            if pos == len(self.data):
                return self.sorted_data[-1]
            else:
                return self.sorted_data[pos]

    @cached_property
    def q1(self):
        rounds = self.rounds
        data = self.sorted_data

        # See: https://en.wikipedia.org/wiki/Quartile#Computing_methods
        if rounds == 1:
            return data[0]
        elif rounds % 2:  # Method 3
            n, q = rounds // 4, rounds % 4
            if q == 1:
                return 0.25 * data[n - 1] + 0.75 * data[n]
            else:
                return 0.75 * data[n] + 0.25 * data[n + 1]
        else:  # Method 2
            return statistics.median(data[: rounds // 2])

    @cached_property
    def q3(self):
        rounds = self.rounds
        data = self.sorted_data

        # See: https://en.wikipedia.org/wiki/Quartile#Computing_methods
        if rounds == 1:
            return data[0]
        elif rounds % 2:  # Method 3
            n, q = rounds // 4, rounds % 4
            if q == 1:
                return 0.75 * data[3 * n] + 0.25 * data[3 * n + 1]
            else:
                return 0.25 * data[3 * n + 1] + 0.75 * data[3 * n + 2]
        else:  # Method 2
            return statistics.median(data[rounds // 2 :])

    @cached_property
    def iqr(self):
        return self.q3 - self.q1

    @property
    def iqr_outliers(self):
        """
        Count of Tukey outliers: what's beyond (Q1 - 1.5IQR, Q3 + 1.5IQR)
        """
        count = 0
        q0 = self.q1 - 1.5 * self.iqr
        q4 = self.q3 + 1.5 * self.iqr
        for val in self.data:
            if val < q0 or val > q4:
                count += 1
        return count

    @cached_property
    def outliers(self):
        return f'{self.stddev_outliers};{self.iqr_outliers}'

    @cached_property
    def ops(self):
        if self.total:
            return self.rounds / self.total
        return 0

    @cached_property
    def median_abs_dev(self):
        return median_abs_dev(self.data)

    def percentile(self, p):
        if not (0 <= p <= 100):
            raise ValueError("p must be in the range [0; 100]")
        return percentile(self.data, p / 100.0)


class Collector:

    def __init__(self):
        """
        https://github.com/psf/pyperf
        https://github.com/psf/pyperf/blob/main/pyperf/_bench.py
        https://github.com/psf/pyperf/blob/main/pyperf/_worker.py#L297
        See also:
        https://github.com/ionelmc/pytest-benchmark/blob/master/src/pytest_benchmark/stats.py
        """
        self.data = defaultdict(list)

    def has(self, key) -> bool:
        return key in self.data

    def get(self, key) -> Stats:
        return Stats(self.data[key])

    def find(self, pattern: str) -> Stats:
        re_pattern = re.compile(pattern)
        stats = Stats()
        for k, v in self.data.items():
            if re_pattern.match(k):
                stats.data += v
        return stats

    def all(self) -> Stats:
        stats = Stats()
        for v in self.data.values():
            stats.data += v
        return stats

    def append(self, key: str, value: float):
        self.data[key].append(value)

    def update(self, other: 'Collector'):
        for k, v in other.data.items():
            self.data[k] += v


def median_abs_dev(values):
    # Median Absolute Deviation
    median = float(statistics.median(values))
    return statistics.median([abs(median - sample) for sample in values])


def percentile(values, p):
    if not isinstance(p, float) or not (0.0 <= p <= 1.0):
        raise ValueError("p must be a float in the range [0.0; 1.0]")

    values = sorted(values)
    if not values:
        raise ValueError("no value")

    k = (len(values) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f != c:
        d0 = values[f] * (c - k)
        d1 = values[c] * (k - f)
        return d0 + d1
    else:
        return values[int(k)]


if hasattr(statistics, 'geometric_mean'):
    _geometric_mean = statistics.geometric_mean
else:
    def _geometric_mean(data):
        # Compute exp(fmean(map(log, data))) using floats
        data = list(map(math.log, data))

        # fmean(data)
        fmean = math.fsum(data) / len(data)

        return math.exp(fmean)


def geometric_mean(data):
    data = list(map(float, data))
    if not data:
        raise ValueError("empty data")
    return _geometric_mean(data)
