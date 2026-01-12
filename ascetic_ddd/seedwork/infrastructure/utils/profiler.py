import pstats
import cProfile as _profile

__all__ = ("profile_it",)


def profile_it(func):

    async def _deco(*a, **kw):
        profiler = _profile.Profile()
        profiler.enable()

        result = await func(*a, **kw)

        profiler.disable()
        profiler.print_stats(sort='cumulative')

        # Below code is to add the stats to the file in human-readable format
        profiler.dump_stats('output.prof')
        stream = open('output.txt', 'w')
        stats = pstats.Stats('output.prof', stream=stream)
        stats.sort_stats('cumtime')
        stats.print_stats()

        return result

    return _deco
