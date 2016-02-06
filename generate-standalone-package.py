import os
import platform
import shutil
import subprocess

from flintrock import __version__ as flintrock_version

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

if __name__ == '__main__':
    import pprint
    pprint.pprint(dict(os.environ))
    operating_system = platform.system()
    machine_type = platform.machine()

    subprocess.run([
            'pyinstaller',
            '--noconfirm',
            '--name', 'flintrock',
            '--additional-hooks-dir', '.',
            'standalone.py'],
        check=True)

    shutil.make_archive(
        base_name=os.path.join(
            THIS_DIR, 'dist',
            'flintrock-{v}-{os}-{m}'.format(
                v=flintrock_version,
                os=operating_system,
                m=machine_type)),
        format='zip',
        root_dir=os.path.join(THIS_DIR, 'dist', 'flintrock'))
