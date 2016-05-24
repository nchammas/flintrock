"""
Download Hadoop from the best available Apache mirror or a custom location.
"""

from __future__ import print_function

import json
import sys
import subprocess

if sys.version_info < (3, 0):
    from urllib import urlretrieve
    from urllib2 import urlopen
else:
    from urllib.request import urlopen, urlretrieve


if __name__ == '__main__':
    hadoop_version = sys.argv[1]
    mirror_url = sys.argv[2].format(v=hadoop_version)
    default_mirror_url = \
        'http://www.apache.org/dyn/closer.lua/hadoop/common/hadoop-{v}/hadoop-{v}.tar.gz?as_json'\
        .format(v=hadoop_version)

    tries = 0
    while tries < 3:
        if mirror_url == default_mirror_url:
            mirror_info = json.loads(urlopen(mirror_url).read().decode('utf-8'))
            file_url = mirror_info['preferred'] + mirror_info['path_info']
        else:
            file_url = mirror_url

        print("Downloading file at:", file_url)
        file_path, _ = \
            urlretrieve(url=file_url, filename="hadoop-{v}.tar.gz".format(v=hadoop_version))
        ret = subprocess.call(['gzip', '--test', file_path])

        if ret == 0:
            break
        else:
            tries += 1
            print("gzip check failed. Retrying download...")

    sys.exit(ret)
