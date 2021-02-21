from datetime import datetime, timedelta, timezone
from flintrock.util import (
    duration_to_timedelta,
    duration_to_expiration,
    spark_hadoop_build_version,
)
from freezegun import freeze_time


def test_duration_to_timedelta():
    assert duration_to_timedelta('1d') == timedelta(days=1)
    assert duration_to_timedelta('3d2h1m') == timedelta(days=3, hours=2, minutes=1)
    assert duration_to_timedelta('4d 2h 1m 5s') == timedelta(days=4, hours=2, minutes=1, seconds=5)
    assert duration_to_timedelta('36h') == timedelta(hours=36)
    assert duration_to_timedelta('7d') == timedelta(days=7)


@freeze_time("2012-01-14")
def test_duration_to_expiration():
    assert duration_to_expiration('5m') == datetime.now(tz=timezone.utc) + timedelta(minutes=5)


def test_spark_hadoop_build_version():
    assert spark_hadoop_build_version('3.1.3') == 'hadoop3.2'
