import functools
from datetime import datetime

def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now().replace(microsecond=0)
        res = func(*args, **kwargs)
        end = datetime.now().replace(microsecond=0)
        print("{f} finished in {t}.".format(f=func.__name__, t=(end - start)))
        return res
    return wrapper
