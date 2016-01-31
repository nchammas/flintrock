import glob
import subprocess
import sys

# External modules
import pytest


@pytest.mark.skipif(sys.version_info < (3, 5), reason="Python 3.5+ is required")
def test_pyinstaller_packaging():
    subprocess.run(
        ['pip', 'install', '-r', 'requirements/maintainer.pip'],
        check=True)
    subprocess.run(
        ['python', 'generate-standalone-package.py'],
        check=True)
    subprocess.run(
        ['./dist/flintrock/flintrock'],
        check=True)
    assert glob.glob('./dist/*.zip')
