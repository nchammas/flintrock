from __future__ import print_function

import argparse
import errno
import os.path
import sys
import subprocess
import time

MAX_TRIES = 5


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('url')
    parser.add_argument('destination_dir')
    args = parser.parse_args()
    return (args.url, args.destination_dir)


if __name__ == '__main__':
    url, destination_dir = parse_args()

    try:
        os.makedirs(destination_dir, mode=0o755)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

    download_path = '{}.download'.format(os.path.basename(destination_dir))

    tries = 0
    while True:
        try:
            if url.startswith('s3://'):
                subprocess.check_call(['aws', 's3', 'cp', url, download_path])
            else:
                subprocess.check_call(['curl', '--location', '--output', download_path, url])
            subprocess.check_call(['gzip', '--test', download_path])
            subprocess.check_call(['tar', 'xzf', download_path, '-C', destination_dir, '--strip-components=1'])
            subprocess.check_call(['rm', download_path])
        except subprocess.CalledProcessError as e:
            print(e, file=sys.stderr)
            if tries < MAX_TRIES:
                tries += 1
                time.sleep(1)
            else:
                print(
                    "Failed to download and unpack '{url}' after {tries} tries."
                    .format(
                        url=url,
                        tries=MAX_TRIES,
                    ),
                    file=sys.stderr,
                )
                sys.exit(1)
        else:
            break
