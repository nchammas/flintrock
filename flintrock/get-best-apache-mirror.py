from __future__ import print_function

import sys
import urllib2
import json

mirror = json.loads(urllib2.urlopen(sys.argv[1]).read())
print(mirror['preferred'] + mirror['path_info'])
