import asyncio
import concurrent.futures
import functools
import json
import os
import posixpath
import shlex
import socket
import subprocess
import tempfile
import textwrap
import time
from collections import namedtuple

# External modules
import paramiko

THIS_DIR = os.path.dirname(os.path.realpath(__file__))


class StorageDirs:
    def __init__(self, *, root, ephemeral, persistent):
        self.root = root
        self.ephemeral = ephemeral
        self.persistent = persistent


# NOTE: We take both IP addresses and host names because we
#       don't understand why Spark doesn't accept IP addresses
#       in its config, yet we prefer IP addresses when
#       connecting to hosts to avoid single-threaded DNS lookups.
#       See: https://github.com/nchammas/flintrock/issues/43
#       See: http://www.dalkescientific.com/writings/diary/archive/2012/01/19/concurrent.futures.html
class FlintrockCluster:
    def __init__(
            self,
            *,
            name,
            ssh_key_pair=None,
            master_ip,
            master_host,
            slave_ips,
            slave_hosts,
            storage_dirs=StorageDirs(root=None, ephemeral=None, persistent=None)):
        self.name = name
        self.ssh_key_pair = ssh_key_pair
        self.master_ip = master_ip
        self.master_host = master_host
        self.slave_ips = slave_ips
        self.slave_hosts = slave_hosts
        self.storage_dirs = storage_dirs

    def generate_template_mapping(self, *, service: str) -> dict:
        """
        Convert a FlintrockCluster instance to a dictionary that we can use
        to fill in template parameters.
        """
        template_mapping = {}

        for k, v in vars(self).items():
            if k == 'slave_hosts':
                template_mapping.update({k: '\n'.join(v)})
            elif k == 'storage_dirs':
                template_mapping.update({
                    'root_dir': v.root + '/' + service,
                    'ephemeral_dirs': ','.join(path + '/' + service for path in v.ephemeral)})

                # If ephemeral storage is available, it replaces the root volume, which is
                # typically persistent. We don't want to mix persistent and ephemeral
                # storage since that causes problems after cluster stop/start; some volumes
                # have leftover data, whereas others start fresh.
                root_ephemeral_dirs = template_mapping['root_dir']
                if template_mapping['ephemeral_dirs']:
                    root_ephemeral_dirs = template_mapping['ephemeral_dirs']
                template_mapping.update({
                    'root_ephemeral_dirs': root_ephemeral_dirs})
            else:
                template_mapping.update({k: v})

        return template_mapping


def format_message(*, message: str, indent: int=4, wrap: int=70):
    """
    Format a lengthy message for printing to screen.
    """
    return textwrap.indent(
        textwrap.fill(
            textwrap.dedent(text=message),
            width=wrap),
        prefix=' ' * indent)


def generate_ssh_key_pair() -> namedtuple('KeyPair', ['public', 'private']):
    """
    Generate an SSH key pair that the cluster can use for intra-cluster
    communication.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        subprocess.check_call(
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


def get_ssh_client(
        *,
        user: str,
        host: str,
        identity_file: str,
        # TODO: Add option to not wait for SSH availability.
        print_status: bool=False) -> paramiko.client.SSHClient:
    """
    Get an SSH client for the provided host, waiting as necessary for SSH to become
    available.
    """
    # paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)

    client = paramiko.client.SSHClient()

    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

    while True:
        try:
            client.connect(
                username=user,
                hostname=host,
                key_filename=identity_file,
                look_for_keys=False,
                timeout=3)
            if print_status:
                print("[{h}] SSH online.".format(h=host))
            break
        # TODO: Somehow rationalize these expected exceptions.
        # TODO: Add some kind of limit on number of failures.
        except socket.timeout as e:
            time.sleep(5)
        except socket.error as e:
            if e.errno != 61:
                raise
            time.sleep(5)
        # We get this exception during startup with CentOS but not Amazon Linux,
        # for some reason.
        except paramiko.ssh_exception.AuthenticationException as e:
            time.sleep(5)

    return client


def ssh_check_output(client: paramiko.client.SSHClient, command: str):
    """
    Run a command via the provided SSH client and return the output captured
    on stdout.

    Raise an exception if the command returns a non-zero code.
    """
    stdin, stdout, stderr = client.exec_command(command, get_pty=True)

    # NOTE: Paramiko doesn't clearly document this, but we must read() before
    #       calling recv_exit_status().
    #       See: https://github.com/paramiko/paramiko/issues/448#issuecomment-159481997
    stdout_output = stdout.read().decode('utf8').rstrip('\n')
    stderr_output = stderr.read().decode('utf8').rstrip('\n')
    exit_status = stdout.channel.recv_exit_status()

    if exit_status:
        # TODO: Return a custom exception that includes the return code.
        #       See: https://docs.python.org/3/library/subprocess.html#subprocess.check_output
        # NOTE: We are losing the output order here since output from stdout and stderr
        #       may be interleaved.
        raise Exception(stdout_output + stderr_output)

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


def _run_asynchronously(*, partial_func: functools.partial, hosts: list):
    """
    Run a function asynchronously against each of the provided hosts.

    This function assumes that partial_func accepts `host` as a keyword argument.
    """
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(len(hosts))

    tasks = []
    for host in hosts:
        # TODO: Use parameter names for run_in_executor() once Python 3.4.4 is released.
        #       Until then, we leave them out to maintain compatibility across Python 3.4
        #       and 3.5.
        # See: http://stackoverflow.com/q/32873974/
        task = loop.run_in_executor(
            executor,
            functools.partial(partial_func, host=host))
        tasks.append(task)

    try:
        loop.run_until_complete(asyncio.gather(*tasks))
        # done, _ = loop.run_until_complete(asyncio.wait(tasks))
        # # Is this the right way to make sure no coroutine failed?
        # for future in done:
        #     future.result()
    finally:
        # TODO: Let KeyboardInterrupt cleanly cancel hung commands.
        #       Currently, we can't do this without dumping a large stack trace or
        #       waiting until the executor threads yield control.
        #       See: http://stackoverflow.com/q/29177490/
        # We shutdown explcitly to make sure threads are cleaned up before shutting
        # the loop down.
        # See: http://stackoverflow.com/a/32615276/
        executor.shutdown(wait=True)
        loop.close()


def provision_cluster(
        *,
        cluster: FlintrockCluster,
        services: list,
        user: str,
        identity_file: str):
    """
    Connect to a freshly launched cluster and install the specified services.
    """
    partial_func = functools.partial(
        provision_node,
        services=services,
        user=user,
        identity_file=identity_file,
        cluster=cluster)
    hosts = [cluster.master_ip] + cluster.slave_ips

    _run_asynchronously(partial_func=partial_func, hosts=hosts)

    # print("All {c} instances provisioned.".format(
    #     c=len(cluster_instances)))

    master_ssh_client = get_ssh_client(
        user=user,
        host=cluster.master_host,
        identity_file=identity_file)

    with master_ssh_client:
        # TODO: This manifest may need to be more full-featured to support
        #       adding nodes to a cluster.
        manifest = {
            'services': [[type(m).__name__, m.version] for m in services]}
        # The manifest tells us how the cluster is configured. We'll need this
        # when we resize the cluster or restart it.
        ssh_check_output(
            client=master_ssh_client,
            command="""
                echo {m} > /home/{u}/.flintrock-manifest.json
            """.format(
                m=shlex.quote(json.dumps(manifest, indent=4, sort_keys=True)),
                u=shlex.quote(user)))

        for service in services:
            service.configure_master(
                ssh_client=master_ssh_client,
                cluster=cluster)

    # NOTE: We sleep here so that the slave services have time to come up.
    #       If we refactor stuff to have a start_slave() that blocks until
    #       the slave is fully up, then we won't need this sleep anymore.
    if services:
        time.sleep(30)

    for service in services:
        service.health_check(master_host=cluster.master_host)


def provision_node(
        *,
        services: list,
        user: str,
        host: str,
        identity_file: str,
        cluster: FlintrockCluster):
    """
    Connect to a freshly launched node, set it up for SSH access, configure ephemeral
    storage, and install the specified services.

    This method is role-agnostic; it runs on both the cluster master and slaves.
    This method is meant to be called asynchronously.
    """
    client = get_ssh_client(
        user=user,
        host=host,
        identity_file=identity_file,
        print_status=True)

    with client:
        ssh_check_output(
            client=client,
            command="""
                set -e

                echo {private_key} > ~/.ssh/id_rsa
                echo {public_key} >> ~/.ssh/authorized_keys

                chmod 400 ~/.ssh/id_rsa
            """.format(
                private_key=shlex.quote(cluster.ssh_key_pair.private),
                public_key=shlex.quote(cluster.ssh_key_pair.public)))

        with client.open_sftp() as sftp:
            sftp.put(
                localpath=os.path.join(THIS_DIR, 'setup-ephemeral-storage.py'),
                remotepath='/tmp/setup-ephemeral-storage.py')

        print("[{h}] Configuring ephemeral storage...".format(h=host))
        # TODO: Print some kind of warning if storage is large, since formatting
        #       will take several minutes (~4 minutes for 2TB).
        storage_dirs_raw = ssh_check_output(
            client=client,
            command="""
                set -e
                python /tmp/setup-ephemeral-storage.py
                rm -f /tmp/setup-ephemeral-storage.py
            """)
        storage_dirs = json.loads(storage_dirs_raw)

        cluster.storage_dirs.root = storage_dirs['root']
        cluster.storage_dirs.ephemeral = storage_dirs['ephemeral']

        # The default CentOS AMIs on EC2 don't come with Java installed.
        java_home = ssh_check_output(
            client=client,
            command="""
                echo "$JAVA_HOME"
            """)

        if not java_home.strip():
            print("[{h}] Installing Java...".format(h=host))

            ssh_check_output(
                client=client,
                command="""
                    set -e

                    sudo yum install -y java-1.7.0-openjdk
                    sudo sh -c "echo export JAVA_HOME=/usr/lib/jvm/jre >> /etc/environment"
                    source /etc/environment
                """)

        for service in services:
            service.install(
                ssh_client=client,
                cluster=cluster)
            service.configure(
                ssh_client=client,
                cluster=cluster)


def start_cluster(*, cluster: FlintrockCluster, user: str, identity_file: str):
    """
    Connect to an existing cluster that has just been started up again and prepare it
    for work.
    """
    master_ssh_client = get_ssh_client(
        user=user,
        host=cluster.master_ip,
        identity_file=identity_file)

    with master_ssh_client:
        manifest_raw = ssh_check_output(
            client=master_ssh_client,
            command="""
                cat /home/{u}/.flintrock-manifest.json
            """.format(u=shlex.quote(user)))
        # TODO: Reconsider where this belongs. In the manifest? We can implement
        #       ephemeral storage support as a Flintrock service, and add methods to
        #       serialize and deserialize critical service info like installed versions
        #       or ephemeral drives to the to/from the manifest.
        #       Another approach is to auto-detect storage inside a start_node()
        #       method. Yet another approach is to determine storage upfront by the
        #       instance type.
        # NOTE: As for why we aren't using ls here, see:
        #       http://mywiki.wooledge.org/ParsingLs
        ephemeral_dirs_raw = ssh_check_output(
            client=master_ssh_client,
            command="""
                shopt -s nullglob
                for f in /media/ephemeral*; do
                    echo "$f"
                done
            """)

    manifest = json.loads(manifest_raw)
    storage_dirs = StorageDirs(
        root='/media/root',
        ephemeral=sorted(ephemeral_dirs_raw.splitlines()),
        persistent=None)
    # TODO: This smells. We are mutating an input to this method.
    cluster.storage_dirs = storage_dirs

    services = []
    for [service_name, version] in manifest['services']:
        # TODO: Expose the classes being used here.
        # TODO: Fix restarted cluster with Spark built from git version
        service = globals()[service_name](version)
        services.append(service)

    partial_func = functools.partial(
        start_node,
        services=services,
        user=user,
        identity_file=identity_file,
        cluster=cluster)
    hosts = [cluster.master_ip] + cluster.slave_ips

    _run_asynchronously(partial_func=partial_func, hosts=hosts)

    master_ssh_client = get_ssh_client(
        user=user,
        host=cluster.master_ip,
        identity_file=identity_file)

    with master_ssh_client:
        for service in services:
            service.configure_master(
                ssh_client=master_ssh_client,
                cluster=cluster)

    # NOTE: We sleep here so that the slave services have time to come up.
    #       If we refactor stuff to have a start_slave() that blocks until
    #       the slave is fully up, then we won't need this sleep anymore.
    if services:
        time.sleep(30)

    for service in services:
        service.health_check(master_host=cluster.master_ip)


def start_node(
        *,
        services: list,
        user: str,
        host: str,
        identity_file: str,
        cluster: FlintrockCluster):
    """
    Connect to an existing node that has just been started up again and prepare it for
    work.

    This method is role-agnostic; it runs on both the cluster master and slaves.
    This method is meant to be called asynchronously.
    """
    ssh_client = get_ssh_client(
        user=user,
        host=host,
        identity_file=identity_file,
        print_status=True)

    with ssh_client:
        # TODO: Consider consolidating ephemeral storage code under a dedicated
        #       Flintrock service.
        if cluster.storage_dirs.ephemeral:
            ssh_check_output(
                client=ssh_client,
                command="""
                    sudo chown "{u}:{u}" {d}
                """.format(
                    u=user,
                    d=' '.join(cluster.storage_dirs.ephemeral)))

        for service in services:
            service.configure(
                ssh_client=ssh_client,
                cluster=cluster)


def run_command_cluster(
        *,
        master_only: bool,
        cluster: FlintrockCluster,
        user: str,
        identity_file: str,
        command: tuple):
    """
    Run a shell command on each node of an existing cluster.

    If master_only is True, then run the comand on the master only.
    """
    if master_only:
        target_hosts = [cluster.master_ip]
    else:
        target_hosts = [cluster.master_ip] + cluster.slave_ips

    print("Running command on {c} instance{s}...".format(
        c=len(target_hosts),
        s='' if len(target_hosts) == 1 else 's'))

    partial_func = functools.partial(
        run_command_node,
        user=user,
        identity_file=identity_file,
        command=command)
    hosts = target_hosts

    _run_asynchronously(partial_func=partial_func, hosts=hosts)


def run_command_node(*, user: str, host: str, identity_file: str, command: tuple):
    """
    Run a shell command on a node.

    This method is role-agnostic; it runs on both the cluster master and slaves.
    This method is meant to be called asynchronously.
    """
    # TODO: Timeout quickly if SSH is not available.
    ssh_client = get_ssh_client(
        user=user,
        host=host,
        identity_file=identity_file)

    print("[{h}] Running command...".format(h=host))

    command_str = ' '.join(command)

    with ssh_client:
        ssh_check_output(
            client=ssh_client,
            command=command_str)

    print("[{h}] Command complete.".format(h=host))


def copy_file_cluster(
        *,
        master_only: bool,
        cluster: FlintrockCluster,
        user: str,
        identity_file: str,
        local_path: str,
        remote_path: str):
    """
    Copy a file to each node of an existing cluster.

    If master_only is True, then copy the file to the master only.
    """
    if master_only:
        target_hosts = [cluster.master_ip]
    else:
        target_hosts = [cluster.master_ip] + cluster.slave_ips

    print("Copying file to {c} instance{s}...".format(
        c=len(target_hosts),
        s='' if len(target_hosts) == 1 else 's'))

    partial_func = functools.partial(
        copy_file_node,
        user=user,
        identity_file=identity_file,
        local_path=local_path,
        remote_path=remote_path)
    hosts = target_hosts

    _run_asynchronously(partial_func=partial_func, hosts=hosts)


def copy_file_node(
        *,
        user: str,
        host: str,
        identity_file: str,
        local_path: str,
        remote_path: str):
    """
    Copy a file to the specified remote path on a node.

    This method is role-agnostic; it runs on both the cluster master and slaves.
    This method is meant to be called asynchronously.
    """
    # TODO: Timeout quickly if SSH is not available.
    ssh_client = get_ssh_client(
        user=user,
        host=host,
        identity_file=identity_file)

    with ssh_client:
        remote_dir = posixpath.dirname(remote_path)

        try:
            ssh_check_output(
                client=ssh_client,
                command="""
                    test -d {path}
                """.format(path=shlex.quote(remote_dir)))
        except Exception as e:
            # TODO: Catch more specific exception.
            raise Exception("Remote directory does not exist: {d}".format(d=remote_dir))

        with ssh_client.open_sftp() as sftp:
            print("[{h}] Copying file...".format(h=host))

            sftp.put(localpath=local_path, remotepath=remote_path)

            print("[{h}] Copy complete.".format(h=host))


# This is necessary down here since we have a circular import dependency between
# core.py and services.py. I've thought about how to remove this circular dependency,
# but for now this seems like what we need to go with.
# Flintrock modules
from .services import HDFS, Spark  # Used by start_cluster()
