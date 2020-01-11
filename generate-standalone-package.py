import os
import platform
import shutil
import subprocess

from flintrock import __version__ as flintrock_version

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

if __name__ == '__main__':
    operating_system = platform.system()
    if operating_system.lower() == 'darwin':
        operating_system = 'macOS'
    machine_type = platform.machine()

    subprocess.run(
        [
            'pyinstaller',
            '--noconfirm',
            '--clean',
            '--name', 'flintrock',
            '--additional-hooks-dir', '.',
            # This hidden import is introduced by botocore.
            # We won't need this when this issue is resolved:
            # https://github.com/pyinstaller/pyinstaller/issues/1844
            '--hidden-import', 'html.parser',
            # This hidden import is also introduced by botocore.
            # It appears to be related to this issue:
            # https://github.com/pyinstaller/pyinstaller/issues/1935
            '--hidden-import', 'configparser',
            'standalone.py'
        ],
        check=True)

    shutil.make_archive(
        base_name=os.path.join(
            THIS_DIR, 'dist',
            'Flintrock-{v}-standalone-{os}-{m}'.format(
                v=flintrock_version,
                os=operating_system,
                m=machine_type)),
        format='zip',
        root_dir=os.path.join(THIS_DIR, 'dist', 'flintrock'))
