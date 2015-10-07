import sys
import shlex
import subprocess
import tempfile
import time
from collections import namedtuple

def ssh_open(instance, user: str, identity_file: str, max_retries = 100, wait_interval = 5):
    """
    Open an SSH connection, returning an active paramiko.client.SSHClient.

    This function will automatically retry the connection attempt for as long as
    it takes to establesh the connection.

    TODO: set max_retries, and report connection errors in some sane fashion to
    the user.
    """
    import paramiko
    import socket

    if identity_file == None:
        print("You must supply a valid SSH identity_file.")
        sys.exit(0)

    num_tries = 0
    while True:
        num_tries += 1
        if num_tries >= max_retries:
            print("Could not establish SSH connection to cluster host {h} after {n} tries.  Aborting.".format(h=instance.public_dns_name, n=max_retries))
            sys.exit(1)

        client = paramiko.client.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

        # Be sure to refresh instance metadata every time through this loop
        instance.update()

        # It takes a little while after an instances is launched or started to
        # get an ip address.  We wait if needed
        if instance.public_dns_name is "":
            time.sleep(wait_interval)
            continue

        try:
            client.connect(
                username=user,
                hostname=instance.public_dns_name,
                key_filename=identity_file,
                look_for_keys=False,  # Needed to prevent locked keys from generating errors on osx
                timeout=5)
            break
        except socket.timeout as e:
            time.sleep(wait_interval)
        except socket.error as e:
            if e.errno != 61:
                raise
            time.sleep(wait_interval)
        except Exception as e:        # TODO: be more precise about which errors we catch
            time.sleep(wait_interval)

    return client

def ssh_check_output(ssh_client: "paramiko.client.SSHClient", command: str, stop_on_failure = True):

    # For debugging
    #host = ssh_client.get_transport().getpeername()[0]
    #print("  SSH [{h}]: {c}".format(h=host, c=command))

    stdin, stdout, stderr = ssh_client.exec_command(command, get_pty=True)
    exit_status = stdout.channel.recv_exit_status()

    if exit_status and stop_on_failure:
        # TODO: Return a custom exception that includes the return code.
        # See: https://docs.python.org/3/library/subprocess.html#subprocess.check_output
        #
        # For now, print out the error and exit.
        print("\n******************************************************************************************")
        print("\nSSH remote command failed:", command)
        print("\nCommand stdout:\n")
        print(stdout.read().decode("utf8").rstrip('\n'))
        print("\nCommand stderr:\n")
        print(stderr.read().decode("utf8").rstrip('\n'))
        print("******************************************************************************************")
        os._exit(1)

    return exit_status


def ssh_login(host, identity_file, ssh_tunnel_ports):
    """
    SSH into a host for interactive use.
    """
    if identity_file is None:
        print("You must specify an SSH identity file.")
        sys.exit(0)

    # SSH tunnels are a convenient, zero-configuration
    # alternative to opening a port using the EC2 security
    # group settings and using iPython notebook over SSL.
    #
    # If the user has requested ssh port forwarding, we set
    # that up here.
    if ssh_tunnel_ports is not None:
        ssh_ports = ssh_tunnel_ports.split(":")
        if len(ssh_ports) != 2:
            print("\nERROR: Could not parse arguments to \'--ssh-tunnel\'.")
            print("       Be sure you use the syntax \'local_port:remote_port\'")
            sys.exit(1)
        print ("\nSSH port forwarding requested.  Remote port " + ssh_ports[1] +
               " will be accessible at http://localhost:" + ssh_ports[0] + '\n')
        try:
            ret = subprocess.call([
                'ssh',
                '-o', 'StrictHostKeyChecking=no',
                '-i', identity_file,
                '-L', '{local}:127.0.0.1:{remote}'.format(local=ssh_ports[0], remote=ssh_ports[1]),
                '{u}@{h}'.format(u = "ec2-user", h = host)])
        except subprocess.CalledProcessError:
            print("\nERROR: Could not establish ssh connection with port forwarding.")
            print("       Check your Internet connection and make sure that the")
            print("       ports you have requested are not already in use.")
            sys.exit(1)
    else:
        ret = subprocess.call([
            'ssh',
            '-o', 'StrictHostKeyChecking=no',
            '-i', identity_file,
            'ec2-user@{h}'.format(h=host)])

    return ret


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
            private_key = private_key_file.read().strip()

        with open(file=tempdir + "/flintrock_rsa.pub") as public_key_file:
            public_key = public_key_file.read().strip()

    return namedtuple('KeyPair', ['public', 'private'])(public_key, private_key)

