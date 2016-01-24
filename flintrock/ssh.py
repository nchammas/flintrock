import asyncio
import errno
import os
import socket
import subprocess
import tempfile
import time
from collections import namedtuple

# External modules
import asyncssh

# Flintrock modules
from .exceptions import SSHError


def generate_ssh_key_pair() -> namedtuple('KeyPair', ['public', 'private']):
    """
    Generate an SSH key pair that the cluster can use for intra-cluster
    communication.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        subprocess.check_call([
            'ssh-keygen',
            '-q',
            '-t', 'rsa',
            '-N', '',
            '-f', os.path.join(tempdir, 'flintrock_rsa'),
            '-C', 'flintrock'])

        with open(file=os.path.join(tempdir, 'flintrock_rsa')) as private_key_file:
            private_key = private_key_file.read()

        with open(file=os.path.join(tempdir, 'flintrock_rsa.pub')) as public_key_file:
            public_key = public_key_file.read()

    return namedtuple('KeyPair', ['public', 'private'])(public_key, private_key)


async def get_ssh_client(
        *,
        user: str,
        host: str,
        identity_file: str,
        print_status: bool=False) -> asyncssh.SSHClientConnection:
    """
    Get an SSH client for the provided host, waiting as necessary for SSH to become
    available.
    """
    # TODO: Add option to not wait for SSH availability.
    client_key = asyncssh.read_private_key(identity_file)
    while True:
        try:
            client = await asyncio.wait_for(
                asyncssh.connect(
                    host=host,
                    username=user,
                    known_hosts=None,
                    client_keys=[client_key]),
                timeout=3)
            if print_status:
                print("[{h}] SSH online.".format(h=host))
            break
        except socket.error as e:
            if e.errno != errno.ECONNREFUSED:
                raise
            else:
                await asyncio.sleep(5)
        except asyncio.TimeoutError as e:
            await asyncio.sleep(5)

    return client


async def ssh_run(
        *,
        client: asyncssh.SSHClientConnection,
        command: str,
        input: str=None,
        timeout: int=None,  # seconds
        check: bool=True):
    """
    Inspired by: https://docs.python.org/3/library/subprocess.html#subprocess.run
    """
    stdin, stdout, stderr = await asyncio.wait_for(
        client.open_session(
            command=command,
            term_type='xterm'),  # This gets us a TTY, which we need for sudo.
        timeout=timeout)

    if input is not None:
        stdin.write(input)
        stdin.write_eof()

    stdout_output = (await stdout.read()).rstrip('\n')
    stderr_output = (await stderr.read()).rstrip('\n')
    exit_status = stdout.channel.get_exit_status()

    if check and exit_status:
        # TODO: Exception attributes for returncode
        raise SSHError(stderr_output)

    # TODO: Return class with stdout, stderr, and returncode.
    return stdout_output


def ssh(*, user: str, host: str, identity_file: str):
    """
    SSH into a host for interactive use.
    """
    ret = subprocess.call([
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-i', identity_file,
        '{u}@{h}'.format(u=user, h=host)])
