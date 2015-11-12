"""
Setup ephemeral storage on a newly launched Linux host.

Since this runs on the remote nodes, it will probably run in Python 2.
"""
from __future__ import print_function

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


def unmount_non_root_devices():
    """
    Unmount any non-root devices.

    For example, EC2 sometimes randomly mounts an ephemeral drive. We might want to
    unmount it so we can format and remount it as we please.
    """
    with open('/proc/mounts') as m:
        mounts = [Mount(*line.split()) for line in m.read().splitlines()]

    for mount in mounts:
        if mount.device_name.startswith('/dev/') and mount.mount_point != '/':
            subprocess.check_call(['sudo', 'umount', mount.device_name])


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
        '--noheadings'])
    block_devices = [BlockDevice(*line.split()) for line in block_devices_raw.splitlines()]
    non_root_block_devices = [bd for bd in block_devices if bd.mount_point != '/']
    return non_root_block_devices


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
            device.name])
        format_processes.append(p)

    for p in format_processes:
        return_code = p.wait()
        if return_code != 0:
            raise Exception(
                "Format process returned non-zero exit code: {c}".format(c=return_code))


def mount_devices(devices):
    """
    Mount the provided devices. Additionally, add the appropriate entries to /etc/fstab
    so that the mounts persist across cluster stop/start.
    """
    mount_prefix = '/media/ephemeral'
    mount_num = 0
    for device in devices:
        mount_name = mount_prefix + str(mount_num)

        subprocess.check_call([
            'sudo', 'mkdir', '-p', mount_name])

        # Replace any existing fstab entries with our own.
        subprocess.check_call(
            """
            grep -v -e "^{device_name}" /etc/fstab | sudo tee /etc/fstab
            """.format(device_name=device.name),
            shell=True)
        subprocess.check_call(
            """
            echo "{fstab_entry}" | sudo tee -a /etc/fstab
            """.format(fstab_entry='   '.join([
                device.name,
                mount_name,
                'ext4',
                'defaults,users,noatime,nodiratime',
                '0',
                '0'])),
            shell=True)

        mount_num += 1

    subprocess.check_call([
        'sudo', 'mount', '--all'])
    # mount changes the owner to root, so we have to set it to what we want here.
    subprocess.check_call(
        'sudo chown "$(logname):$(logname)" {m}*'.format(m=mount_prefix),
        shell=True)


if __name__ == '__main__':
    unmount_non_root_devices()
    non_root_block_devices = get_non_root_block_devices()
    format_devices(non_root_block_devices)
    mount_devices(non_root_block_devices)
