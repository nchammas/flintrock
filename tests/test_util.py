from datetime import timedelta
from flintrock.util import duration_to_timedelta


def test_duration_to_timedelta():
    assert duration_to_timedelta('1d') == timedelta(days=1)
    assert duration_to_timedelta('3d2h1m') == timedelta(days=3, hours=2, minutes=1)
    assert duration_to_timedelta('4d 2h 1m 5s') == timedelta(days=4, hours=2, minutes=1, seconds=5)
    assert duration_to_timedelta('36h') == timedelta(hours=36)
    assert duration_to_timedelta('7d') == timedelta(days=7)
