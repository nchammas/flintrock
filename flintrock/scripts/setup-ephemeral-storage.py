"""
Setup ephemeral storage on a newly launched Linux host.

This script was developed against EC2, where ephemeral volumes are by
default haphazardly and inconsistently mounted. Therefore, we unmount
all volumes that we detect and explicitly format and remount them as
we please.

The resulting structure we create is as follows:

    /media
        /root: The instance's root volume.
        /ephemeral[0-N]: Instance store volumes.
        /persistent[0-N]: EBS volumes.
        /tmp: A temporary directory with lots of space.

WARNING: Be conscious about what this script prints to stdout, as that
         output is parsed by Flintrock.
"""
from __future__ import print_function
from __future__ import unicode_literals

import json
import platform
import subprocess
import sys

from collections import namedtuple

# Taken from: http://man7.org/linux/man-pages/man5/fstab.5.html
Mount = namedtuple(
    'Mount', [
        'device_name',
        'mount_point',
        'filesystem_type',
        'mount_options',
        'dump',
        'pass_number'
    ])

BlockDevice = namedtuple(
    'BlockDevice', sorted([
        'kname',
        'mountpoint',
        'size',
    ]))
BlockDevice.__new__.__defaults__ = (None, ) * len(BlockDevice._fields)


def device_pairs_to_tuple(pairs):
    device_dict = {}
    for pair in pairs:
        key, value = pair.split('=')
        key = key.lower()
        value = value.strip('"').lower()
        device_dict.update({key: value})
    return BlockDevice(**device_dict)


def get_non_root_block_devices():
    """
    Get all the non-root block devices available to the host.

    These are the devices we're going to format and mount for use.
    """
    block_devices_raw = subprocess.check_output([
        'lsblk',
        '--ascii',
        '--pairs',
        '--bytes',
        '--paths',
        '--output', 'KNAME,MOUNTPOINT,SIZE',
        # --inverse and --nodeps make sure that
        #   1) we get the mount points for devices that have holder devices
        #   2) we don't get the holder devices themselves
        '--inverse',
        '--nodeps',
        '--noheadings',
    ]).decode('utf-8')
    block_devices = [
        device_pairs_to_tuple(line.split())
        for line in block_devices_raw.splitlines()
    ]
    non_root_block_devices = [
        device for device in block_devices
        if device.mountpoint != '/'
    ]
    # Skip tiny devices, like the 1M devices that show up on
    # m5 instances on EC2.
    # See: https://github.com/nchammas/flintrock/issues/256
    non_trivial_non_root_block_devices = [
        device for device in non_root_block_devices
        if int(device.size) >= 1024 ** 3
    ]
    return non_trivial_non_root_block_devices


def unmount_devices(devices):
    """
    Unmount the provided devices.
    """
    with open('/proc/mounts') as m:
        mounts = [Mount(*line.split()) for line in m.read().splitlines()]

    for mount in mounts:
        if mount.device_name in [d.kname for d in devices]:
            subprocess.check_output(['sudo', 'umount', mount.device_name])


def format_devices(devices):
    """
    Create an ext4 filesystem on the provided devices.
    """
    format_processes = []
    for device in devices:
        p = subprocess.Popen([
            'sudo', 'mkfs.ext4',
            '-F',
            '-E',
            'lazy_itable_init=0,lazy_journal_init=0',
            device.kname],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        format_processes.append(p)

    for p in format_processes:
        stdout_raw, stderr_raw = p.communicate()
        stdout, stderr = stdout_raw.decode('utf-8'), stderr_raw.decode('utf-8')  # noqa
        return_code = p.returncode
        if return_code != 0:
            raise Exception(
                "Format process returned non-zero exit code: {code}\n{error}"
                .format(
                    code=return_code,
                    error=stderr))


def mount_devices(devices):
    """
    Mount the provided devices at the provided mount points.

    Additionally, add the appropriate entries to /etc/fstab so that the mounts
    persist across cluster stop/start.
    """
    for device in devices:
        subprocess.check_output([
            'sudo', 'mkdir', '-p', device.mountpoint])

        # Replace any existing fstab entries with our own.
        subprocess.check_output(
            """
            grep -v -e "^{device_name}" /etc/fstab | sudo tee /etc/fstab
            """.format(device_name=device.kname),
            shell=True)
        subprocess.check_output(
            """
            echo "{fstab_entry}" | sudo tee -a /etc/fstab
            """.format(fstab_entry='   '.join([
                device.kname,
                device.mountpoint,
                'ext4',
                'defaults,users,noatime',
                '0',
                '0'])),
            shell=True)

        subprocess.check_output([
            'sudo', 'mount', '--source', device.kname])
        # NOTE: `mount` changes the mount point owner to root, so we have
        #       to set it to what we want here, after `mount` runs.
        subprocess.check_output(
            'sudo chown "$(logname):$(logname)" {m}'.format(m=device.mountpoint),
            shell=True)


def create_root_dir():
    """
    Create a folder that services like HDFS and Spark can refer to to access
    local storage on the root volume.
    """
    path = '/media/root'
    subprocess.check_output([
        'sudo', 'mkdir', '-p', path])
    subprocess.check_output(
        'sudo chown "$(logname):$(logname)" {p}'.format(p=path),
        shell=True)
    return path


def create_tmp_dir(target):
    """
    Create a folder that services can use as a temporary directory for big files.
    """
    path = '/media/tmp'
    subprocess.check_output([
        'sudo', 'ln', '-s', target, path])
    subprocess.check_output(
        'sudo chown "$(logname):$(logname)" {p}'.format(p=path),
        shell=True)
    return path


if __name__ == '__main__':
    if sys.version_info < (2, 7) or ((3, 0) <= sys.version_info < (3, 4)):
        raise Exception(
            "This script is only supported on Python 2.7+ and 3.4+. "
            "You are running Python {v}.".format(v=platform.python_version()))

    non_root_block_devices = get_non_root_block_devices()

    # NOTE: For now we are assuming that all non-root devices are ephemeral devices.
    #       We're going to assign them the mount points we want them to have once we're
    #       done with the unmount -> format -> mount cycle.
    ephemeral_devices = []
    for (num, device) in enumerate(sorted(non_root_block_devices, key=lambda d: d.kname)):
        ephemeral_devices.append(
            BlockDevice(
                kname=device.kname,
                mountpoint='/media/ephemeral' + str(num)))

    unmount_devices(ephemeral_devices)
    format_devices(ephemeral_devices)
    mount_devices(ephemeral_devices)

    root_dir = create_root_dir()
    if ephemeral_devices:
        tmp_dir = ephemeral_devices[0].mountpoint
    else:
        tmp_dir = '/tmp'
    create_tmp_dir(tmp_dir)

    print(json.dumps(
        {
            'root': root_dir,
            'ephemeral': [d.mountpoint for d in ephemeral_devices]
        }))
