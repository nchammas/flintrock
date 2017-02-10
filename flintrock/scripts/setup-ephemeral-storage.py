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
    'BlockDevice', [
        'name',
        'mount_point'
    ])
BlockDevice.__new__.__defaults__ = (None, None)


def get_non_root_block_devices():
    """
    Get all the non-root block devices available to the host.

    These are the devices we're going to format and mount for use.
    """
    block_devices_raw = subprocess.check_output([
        'lsblk',
        '--ascii',
        '--paths',
        '--output', 'KNAME,MOUNTPOINT',
        # --inverse and --nodeps make sure that
        #   1) we get the mount points for devices that have holder devices
        #   2) we don't get the holder devices themselves
        '--inverse',
        '--nodeps',
        '--noheadings']).decode('utf-8')
    block_devices = [BlockDevice(*line.split()) for line in block_devices_raw.splitlines()]
    non_root_block_devices = [bd for bd in block_devices if bd.mount_point != '/']
    return non_root_block_devices


def unmount_devices(devices):
    """
    Unmount the provided devices.
    """
    with open('/proc/mounts') as m:
        mounts = [Mount(*line.split()) for line in m.read().splitlines()]

    for mount in mounts:
        if mount.device_name in [d.name for d in devices]:
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
            device.name],
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
            'sudo', 'mkdir', '-p', device.mount_point])

        # Replace any existing fstab entries with our own.
        subprocess.check_output(
            """
            grep -v -e "^{device_name}" /etc/fstab | sudo tee /etc/fstab
            """.format(device_name=device.name),
            shell=True)
        subprocess.check_output(
            """
            echo "{fstab_entry}" | sudo tee -a /etc/fstab
            """.format(fstab_entry='   '.join([
                device.name,
                device.mount_point,
                'ext4',
                'defaults,users,noatime',
                '0',
                '0'])),
            shell=True)

        subprocess.check_output([
            'sudo', 'mount', '--source', device.name])
        # NOTE: `mount` changes the mount point owner to root, so we have
        #       to set it to what we want here, after `mount` runs.
        subprocess.check_output(
            'sudo chown "$(logname):$(logname)" {m}'.format(m=device.mount_point),
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
    for (num, device) in enumerate(sorted(non_root_block_devices, key=lambda d: d.name)):
        ephemeral_devices.append(
            BlockDevice(
                name=device.name,
                mount_point='/media/ephemeral' + str(num)))

    unmount_devices(ephemeral_devices)
    format_devices(ephemeral_devices)
    mount_devices(ephemeral_devices)

    root_dir = create_root_dir()

    print(json.dumps(
        {
            'root': root_dir,
            'ephemeral': [d.mount_point for d in ephemeral_devices]
        }))
