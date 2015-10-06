import shlex
import subprocess
import tempfile
from collections import namedtuple

def ssh(host, identity_file):
    """
    SSH into a host for interactive use.
    """
    if identity_file is None:
        print("You must specify an SSH identity file.")
        sys.exit(0)

    ret = subprocess.call([
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-i', identity_file,
        'ec2-user@{h}'.format(h=host)])


def generate_ssh_key_pair() -> namedtuple('KeyPair', ['public', 'private']):
    """
    Generate an SSH key pair that the cluster can use for intra-cluster
    communication.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        ret = subprocess.check_call(
            """
            ssh-keygen -q -t rsa -N '' -f {key_file} -C flintrock
            """.format(
                key_file=shlex.quote(tempdir + "/flintrock_rsa")),
            shell=True)

        with open(file=tempdir + "/flintrock_rsa") as private_key_file:
            private_key = private_key_file.read()

        with open(file=tempdir + "/flintrock_rsa.pub") as public_key_file:
            public_key = public_key_file.read()

    return namedtuple('KeyPair', ['public', 'private'])(public_key, private_key)



def ssh_check_output(client: "paramiko.client.SSHClient", command: str):
    """
    Run a command via the provided SSH client and return the output captured
    on stdout.

    Raise an exception if the command returns a non-zero code.
    """
    stdin, stdout, stderr = client.exec_command(command, get_pty=True)
    exit_status = stdout.channel.recv_exit_status()

    if exit_status:
        # TODO: Return a custom exception that includes the return code.
        # See: https://docs.python.org/3/library/subprocess.html#subprocess.check_output
        raise Exception(
            stdout.read().decode("utf8").rstrip('\n') +
            stderr.read().decode("utf8").rstrip('\n'))

    return stdout.read().decode("utf8").rstrip('\n')

