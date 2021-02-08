import glob
import os
import shutil
import subprocess

from conftest import aws_credentials_required

# External modules
import pytest


def pyinstaller_exists():
    return shutil.which('pyinstaller') is not None


# PyTest doesn't let you place skipif markers on fixures. Otherwise,
# we'd ideally be able to do that and all the dependent tests would be
# skipped automatically.
@pytest.fixture(scope='session')
def pyinstaller_flintrock():
    flintrock_executable_path = './dist/flintrock/flintrock'
    p = subprocess.run([
        'python', 'generate-standalone-package.py'
    ])
    assert p.returncode == 0
    assert glob.glob('./dist/*.zip')
    assert os.path.isfile(flintrock_executable_path)
    return flintrock_executable_path


@pytest.mark.skipif(not pyinstaller_exists(), reason="PyInstaller is required")
def test_pyinstaller_flintrock_help(pyinstaller_flintrock):
    p = subprocess.run(
        # Without explicitly setting the locale here, Click will complain
        # when this test is run via GitHub Desktop that the locale is
        # misconfigured.
        """
        export LANG=en_US.UTF-8
        {flintrock_executable}
        """.format(
            flintrock_executable=pyinstaller_flintrock
        ),
        shell=True)
    assert p.returncode == 0


@pytest.mark.skipif(not pyinstaller_exists(), reason="PyInstaller is required")
@aws_credentials_required
def test_pyinstaller_flintrock_describe(pyinstaller_flintrock):
    # This test picks up some PyInstaller packaging issues that are not
    # exposed by the help test.
    p = subprocess.run(
        # Without explicitly setting the locale here, Click will complain
        # when this test is run via GitHub Desktop that the locale is
        # misconfigured.
        """
        export LANG=en_US.UTF-8
        {flintrock_executable} describe
        """.format(
            flintrock_executable=pyinstaller_flintrock,
        ),
        shell=True)
    assert p.returncode == 0
