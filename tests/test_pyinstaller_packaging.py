import glob
import shutil
import subprocess
import sys

# External modules
import pytest


def pyinstaller_exists():
    return shutil.which('pyinstaller') is not None


@pytest.mark.skipif(sys.version_info < (3, 5), reason="Python 3.5+ is required")
@pytest.mark.skipif(not pyinstaller_exists(), reason="PyInstaller is required")
def test_pyinstaller_packaging():
    subprocess.run(
        ['python', 'generate-standalone-package.py'],
        check=True)
    subprocess.run(
        # Without explicitly setting the locale here, Click will complain
        # when this test is run via GitHub Desktop that the locale is
        # misconfigured.
        """
        export LANG=en_US.UTF-8
        ./dist/flintrock/flintrock
        """,
        shell=True,
        check=True)
    assert glob.glob('./dist/*.zip')
