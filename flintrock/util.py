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
