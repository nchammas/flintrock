import glob
import subprocess
import sys

# External modules
import pytest


def pyinstaller_exists():
    s = subprocess.run(['command', '-v', 'pyinstaller'])
    return s.returncode == 0


@pytest.mark.skipif(sys.version_info < (3, 5), reason="Python 3.5+ is required")
@pytest.mark.skipif(not pyinstaller_exists(), reason="PyInstaller is required")
def test_pyinstaller_packaging():
    subprocess.run(
        ['python', 'generate-standalone-package.py'],
        check=True)
    subprocess.run(
        ['./dist/flintrock/flintrock'],
        check=True)
    assert glob.glob('./dist/*.zip')
