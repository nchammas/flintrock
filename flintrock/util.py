import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

FROZEN = getattr(sys, 'frozen', False)


def get_subprocess_env() -> dict:
    """
    Get the environment we want to use when making subprocess calls.
    This takes care of details that affect subprocess calls made from
    PyInstaller-packaged versions of Flintrock.

    For more information see: https://github.com/pyinstaller/pyinstaller/blob/develop/doc/runtime-information.rst#ld_library_path--libpath-considerations
    """
    env = dict(os.environ)
    if FROZEN:
        env['LD_LIBRARY_PATH'] = env.get('LD_LIBRARY_PATH_ORIG', '')
    return env


def duration_to_timedelta(duration_string):
    """
    Convert a time duration string (e.g. 3h 4m 10s) into a timedelta
    """

    duration_string = duration_string.lower()

    total_seconds = Decimal('0')

    prev_num = []
    for character in duration_string:
        if character.isalpha():
            if prev_num:
                num = Decimal(''.join(prev_num))
                if character == 'd':
                    total_seconds += num * 60 * 60 * 24
                elif character == 'h':
                    total_seconds += num * 60 * 60
                elif character == 'm':
                    total_seconds += num * 60
                elif character == 's':
                    total_seconds += num
                prev_num = []

        elif character.isnumeric() or character == '.':
            prev_num.append(character)

    return timedelta(seconds=float(total_seconds))


def duration_to_expiration(duration_string):
    default_duration = timedelta(days=7)

    if not duration_string:
        expiration = datetime.now(tz=timezone.utc) + default_duration
    else:
        expiration = datetime.now(tz=timezone.utc) + duration_to_timedelta(duration_string)

    return expiration


def spark_hadoop_build_version(hadoop_version: str) -> str:
    """
    Given a Hadoop version, determine the Hadoop build of Spark to use.
    """
    hadoop_version = tuple(map(int, hadoop_version.split('.')))
    if hadoop_version < (2, 7):
        return 'hadoop2.6'
    elif (2, 7) <= hadoop_version < (3, 0):
        return 'hadoop2.7'
    elif (3, 0) <= hadoop_version:
        return 'hadoop3.2'
