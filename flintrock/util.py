import os
import sys

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
