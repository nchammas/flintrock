import asyncio
import concurrent.futures
import functools
import json
import os
import posixpath
import shlex
import sys
import time

# Flintrock modules
from .ssh import get_ssh_client, ssh_check_output, ssh

FROZEN = getattr(sys, 'frozen', False)

if FROZEN:
    THIS_DIR = sys._MEIPASS
else:
    THIS_DIR = os.path.dirname(os.path.realpath(__file__))

SCRIPTS_DIR = os.path.join(THIS_DIR, 'scripts')


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
            # master_ip,
            # master_host,
            # slave_ips,
            # slave_hosts,
            storage_dirs=StorageDirs(root=None, ephemeral=None, persistent=None)):
        self.name = name
        self.ssh_key_pair = ssh_key_pair
        # self.master_ip = None
        # self.master_host = None
        # self.slave_ips = []
        # self.slave_hosts = []
        self.storage_dirs = storage_dirs

    @property
    def master_ip(self) -> str:
        """
        The IP address of the master.

        Providers must override this property since it is typically derived from
        an underlying object, like an EC2 instance.
        """
        raise NotImplementedError

    @property
    def master_host(self) -> str:
        """
        The hostname of the master.

        Providers must override this property since it is typically derived from
        an underlying object, like an EC2 instance.
        """
        raise NotImplementedError

    @property
    def slave_ips(self) -> 'List[str]':
        """
        A list of the IP addresses of the slaves.

        Providers must override this property since it is typically derived from
        an underlying object, like an EC2 instance.
        """
        raise NotImplementedError

    @property
    def slave_hosts(self) -> 'List[str]':
        """
        A list of the hostnames of the slaves.

        Providers must override this property since it is typically derived from
        an underlying object, like an EC2 instance.
        """
        raise NotImplementedError

    def destroy_check(self):
        """
        Check that the cluster is in a state in which it can be destroyed.

        Providers should override this method since we have no way to perform
        this check in a provider-agnostic way.
        """
        pass

    def destroy(self):
        """
        Destroy the cluster and any resources created specifically to support
        it.

        Providers should override this method since we have no way to destroy a
        cluster in a provider-agnostic way.

        Nonetheless, this method should be called before the underlying provider
        destroys the nodes. That way, if we ever add cleanup logic here to destroy
        resources external to the cluster it will get executed correctly.
        """
        pass

    def start_check(self):
        """
        Check that the cluster is in a state in which it can be started.

        The interface can use this method to decide whether it needs to prompt
        the user for confirmation. If the cluster cannot be started (e.g.
        because it's already running) then we don't want to show a prompt.

        Providers should override this method since we have no way to perform
        this check in a provider-agnostic way.
        """
        pass

    def start(self, *, user: str, identity_file: str):
        """
        Start up all the services installed on the cluster.

        This method assumes that the nodes constituting cluster were just
        started up by the provider (e.g. EC2, GCE, etc.) they're hosted on
        and are running.
        """
        master_ssh_client = get_ssh_client(
            user=user,
            host=self.master_ip,
            identity_file=identity_file,
            wait=True,
            print_status=False)

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
        self.storage_dirs = storage_dirs

        services = []
        for [service_name, manifest] in manifest['services']:
            # TODO: Expose the classes being used here.
            service = globals()[service_name](**manifest)
            services.append(service)

        partial_func = functools.partial(
            start_node,
            services=services,
            user=user,
            identity_file=identity_file,
            cluster=self)
        hosts = [self.master_ip] + self.slave_ips

        _run_asynchronously(partial_func=partial_func, hosts=hosts)

        master_ssh_client = get_ssh_client(
            user=user,
            host=self.master_ip,
            identity_file=identity_file)

        with master_ssh_client:
            for service in services:
                service.configure_master(
                    ssh_client=master_ssh_client,
                    cluster=self)

        # NOTE: We sleep here so that the slave services have time to come up.
        #       If we refactor stuff to have a start_slave() that blocks until
        #       the slave is fully up, then we won't need this sleep anymore.
        if services:
            time.sleep(30)

        for service in services:
            service.health_check(master_host=self.master_ip)

    def stop_check(self):
        """
        Check that the cluster is in a state in which it can be stopped.

        Providers should override this method since we have no way to perform
        this check in a provider-agnostic way.
        """
        pass

    def stop(self):
        """
        Prepare the cluster to be stopped by the underlying provider.

        There's currently nothing to do here, but this method should be called
        before the underlying provider stops the nodes.
        """
        pass

    def run_command_check(self):
        """
        Check that the cluster is in a state that supports running commands.

        Providers should override this method since we have no way to perform
        this check in a provider-agnostic way.
        """
        pass

    def run_command(
            self,
            *,
            master_only: bool,
            user: str,
            identity_file: str,
            command: tuple):
        """
        Run a shell command on each node of an existing cluster.

        If master_only is True, then run the comand on the master only.
        """
        if master_only:
            target_hosts = [self.master_ip]
        else:
            target_hosts = [self.master_ip] + self.slave_ips

        partial_func = functools.partial(
            run_command_node,
            user=user,
            identity_file=identity_file,
            command=command)
        hosts = target_hosts

        _run_asynchronously(partial_func=partial_func, hosts=hosts)

    def copy_file_check(self):
        """
        Check that the cluster is in a state in which files can be copied to
        it.

        Providers should override this method since we have no way to perform
        this check in a provider-agnostic way.
        """
        pass

    def copy_file(
            self,
            *,
            master_only: bool,
            user: str,
            identity_file: str,
            local_path: str,
            remote_path: str):
        """
        Copy a file to each node of an existing cluster.

        If master_only is True, then copy the file to the master only.
        """
        if master_only:
            target_hosts = [self.master_ip]
        else:
            target_hosts = [self.master_ip] + self.slave_ips

        partial_func = functools.partial(
            copy_file_node,
            user=user,
            identity_file=identity_file,
            local_path=local_path,
            remote_path=remote_path)
        hosts = target_hosts

        _run_asynchronously(partial_func=partial_func, hosts=hosts)

    def login(
            self,
            *,
            user: str,
            identity_file: str):
        """
        Interactively SSH into the cluster master.
        """
        ssh(
            host=self.master_ip,
            user=user,
            identity_file=identity_file)

    def generate_template_mapping(self, *, service: str) -> dict:
        """
        Generate a template mapping from a FlintrockCluster instance that we can use
        to fill in template parameters.
        """
        root_dir = posixpath.join(self.storage_dirs.root, service)
        ephemeral_dirs = ','.join(posixpath.join(path, service) for path in self.storage_dirs.ephemeral)

        template_mapping = {
            'master_ip': self.master_ip,
            'master_host': self.master_host,
            'slave_ips': '\n'.join(self.slave_ips),
            'slave_hosts': '\n'.join(self.slave_hosts),
            'root_dir': root_dir,
            'ephemeral_dirs': ephemeral_dirs,

            # If ephemeral storage is available, it replaces the root volume, which is
            # typically persistent. We don't want to mix persistent and ephemeral
            # storage since that causes problems after cluster stop/start; some volumes
            # have leftover data, whereas others start fresh.
            'root_ephemeral_dirs': ephemeral_dirs if ephemeral_dirs else root_dir,
        }

        return template_mapping


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

    master_ssh_client = get_ssh_client(
        user=user,
        host=cluster.master_host,
        identity_file=identity_file)

    with master_ssh_client:
        manifest = {
            'services': [[type(m).__name__, m.manifest] for m in services]}
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
        wait=True)

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
                localpath=os.path.join(SCRIPTS_DIR, 'setup-ephemeral-storage.py'),
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
        wait=True)

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


def run_command_node(*, user: str, host: str, identity_file: str, command: tuple):
    """
    Run a shell command on a node.

    This method is role-agnostic; it runs on both the cluster master and slaves.
    This method is meant to be called asynchronously.
    """
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
