"""
Download Hadoop from the best available Apache mirror.
"""

from __future__ import print_function

import json
import os
import sys
import subprocess

if sys.version_info < (3, 0):
    from urllib import urlretrieve
    from urllib2 import urlopen
else:
    from urllib.request import urlopen, urlretrieve


if __name__ == '__main__':
    arg_names = ['hadoop_version', 'custom_mirror_url']
    args = dict(zip(arg_names, sys.argv))

    if 'custom_mirror_url' in args.keys:
        mirror_url = args['custom_mirror_url'].format(v=args['hadoop_version'])
    else:
        mirror_url = "http://www.apache.org/dyn/closer.lua/hadoop/common/hadoop-{v}/hadoop-{v}.tar.gz?as_json"\
            .format(v=args['hadoop_version'])

    tries = 0
    while tries < 3:
        mirror_info = json.loads(urlopen(mirror_url).read().decode('utf-8'))
        file_url = mirror_info['preferred'] + mirror_info['path_info']
        file_name = os.path.basename(mirror_info['path_info'])
        print('Downloading file at:', file_url)

        file_path, _ = urlretrieve(url=file_url, filename=file_name)
        ret = subprocess.call(['gzip', '--test', file_path])

        if ret == 0:
            break
        else:
            tries += 1
            print("gzip check failed. Retrying download...")

    sys.exit(ret)
