import compileall
import os
import subprocess

# External modules
import yaml

FLINTROCK_ROOT_DIR = (
    os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__))))

TEST_TARGETS = [
    'setup.py',
    'flintrock/',
    'tests/']

TEST_PATHS = [
    os.path.join(FLINTROCK_ROOT_DIR, path) for path in TEST_TARGETS]


def test_code_compiles():
    for path in TEST_PATHS:
        if os.path.isdir(path):
            result = compileall.compile_dir(path)
        else:
            result = compileall.compile_file(path)
        # NOTE: This is not publicly documented, but a return of 1 means
        #       the compilation succeeded.
        #       See: http://bugs.python.org/issue25768
        assert result == 1


def test_flake8():
    ret = subprocess.call(['flake8'], cwd=FLINTROCK_ROOT_DIR)
    assert ret == 0


def test_config_template_is_valid():
    config_template = os.path.join(FLINTROCK_ROOT_DIR, 'flintrock', 'config.yaml.template')
    with open(config_template) as f:
        yaml.safe_load(f)
