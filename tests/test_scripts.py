import os
import subprocess
import tempfile

import pytest


@pytest.fixture(scope='module')
def tgz_file(request):
    with tempfile.NamedTemporaryFile() as source_file:
        source_file.file.write('Hi!'.encode('utf-8'))
        tgz_file_name = source_file.name + '.tgz'
        subprocess.run(
            ['tar', 'czf', tgz_file_name, source_file.name],
            check=True,
        )

    def destroy():
        subprocess.run(
            ['rm', tgz_file_name],
            check=True,
        )
    request.addfinalizer(destroy)

    return tgz_file_name


@pytest.mark.parametrize('python', ['python', 'python2'])
def test_download_package(python, project_root_dir, tgz_file):
    with tempfile.TemporaryDirectory() as temp_dir:
        subprocess.run(
            [
                python,
                os.path.join(project_root_dir, 'flintrock/scripts/download-package.py'),
                'file://' + tgz_file,
                temp_dir,
            ],
            check=True,
        )
