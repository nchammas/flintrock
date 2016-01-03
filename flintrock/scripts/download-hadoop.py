"""
Download Hadoop from the best available Apache mirror.

Python 2
"""

from __future__ import print_function

import os
import sys
import subprocess
import urllib
import urllib2
import json

hadoop_version = sys.argv[1]
apache_mirror_url = "http://www.apache.org/dyn/closer.lua/hadoop/common/hadoop-{v}/hadoop-{v}.tar.gz?as_json".format(v=hadoop_version)

tries = 0
while tries < 3:
    mirror_info = json.loads(urllib2.urlopen(apache_mirror_url).read())
    file_url = mirror_info['preferred'] + mirror_info['path_info']
    file_name = os.path.basename(mirror_info['path_info'])
    print('Downloading file at:', file_url)

    file_path, _ = urllib.urlretrieve(url=file_url, filename=file_name)
    ret = subprocess.call(['gzip', '--test', file_path])

    if ret == 0:
        break
    else:
        tries += 1
        print("gzip check failed. Retrying download...")

sys.exit(ret)
