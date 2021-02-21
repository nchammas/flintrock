import concurrent.futures
import functools
import json
import os
import posixpath
import shlex
import sys
import logging
from concurrent.futures import FIRST_EXCEPTION

# External modules
import paramiko

# Flintrock modules
from .ssh import get_ssh_client, ssh_check_output, ssh, SSHKeyPair
from .exceptions import SSHError

FROZEN = getattr(sys, 'frozen', False)

if FROZEN:
    THIS_DIR = sys._MEIPASS
else:
    THIS_DIR = os.path.dirname(os.path.realpath(__file__))

SCRIPTS_DIR = os.path.join(THIS_DIR, 'scripts')


logger = logging.getLogger('flintrock.core')


class StorageDirs:
    def __init__(self, *, root, ephemeral, persistent):
        self.root = root
        self.ephemeral = ephemeral
        self.persistent = persistent


# TODO: Implement concept of ClusterNode. (?) That way we can
#       define a cluster as having several nodes, and implement
#       actions as `for node in nodes: node.action()`.
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
        storage_dirs=StorageDirs(root=None, ephemeral=None, persistent=None),
    ):
        self.name = name
        self.ssh_key_pair = ssh_key_pair
        self.storage_dirs = storage_dirs
        self.java_version = None
        self.services = []

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
    def private_network(self) -> bool:
        """
        Indicate if this cluster runs on a private network.

        Providers must override this property since it is typically derived from
        an underlying object, like the VPC subnet of an EC2 Instance.
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

    @property
    def num_masters(self) -> int:
        """
        How many masters the cluster has.

        This normally just equals 1, but in cases where the cluster master
        has been destroyed this should return 0.

        Providers must override this property.
        """
        raise NotImplementedError

    @property
    def num_slaves(self) -> int:
        """
        How many slaves the cluster has.

        This is typically just len(self.slave_ips), but we need a separate
        property because slave IPs are not available when the cluster is
        stopped, and sometimes in that situation we still want to know how
        many slaves there are.

        Providers must override this property.
        """
        raise NotImplementedError

    def load_manifest(self, *, user: str, identity_file: str):
        """
        Load a cluster's manifest from the master. This will populate information
        about installed services and configured storage.

        Providers shouldn't need to override this method.
        """
        if not self.master_ip:
            return

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
                    cat "$HOME/.flintrock-manifest.json"
                """)
            # TODO: Would it be better if storage (ephemeral and otherwise) was
            #       implemented as a Flintrock service and tracked in the manifest?
            ephemeral_dirs_raw = ssh_check_output(
                client=master_ssh_client,
                # It's generally safer to avoid using ls:
                # http://mywiki.wooledge.org/ParsingLs
                command="""
                    shopt -s nullglob
                    for f in /media/ephemeral*; do
                        echo "$f"
                    done
                """)

        manifest = json.loads(manifest_raw)

        self.ssh_key_pair = SSHKeyPair(
            public=manifest['ssh_key_pair']['public'],
            private=manifest['ssh_key_pair']['private'],
        )

        self.java_version = manifest['java_version']

        services = []
        for [service_name, manifest] in manifest['services']:
            # TODO: Expose the classes being used here.
            service = globals()[service_name](**manifest)
            services.append(service)
        self.services = services

        storage_dirs = StorageDirs(
            root='/media/root',
            ephemeral=sorted(ephemeral_dirs_raw.splitlines()),
            persistent=None)
        self.storage_dirs = storage_dirs

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
        self.load_manifest(user=user, identity_file=identity_file)

        partial_func = functools.partial(
            start_node,
            services=self.services,
            user=user,
            identity_file=identity_file,
            cluster=self)
        hosts = [self.master_ip] + self.slave_ips

        run_against_hosts(partial_func=partial_func, hosts=hosts)

        master_ssh_client = get_ssh_client(
            user=user,
            host=self.master_ip,
            identity_file=identity_file)

        with master_ssh_client:
            for service in self.services:
                service.configure_master(
                    ssh_client=master_ssh_client,
                    cluster=self)

        for service in self.services:
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

    def add_slaves_check(self):
        pass

    def add_slaves(self, *, user: str, identity_file: str, new_hosts: list):
        """
        Add new slaves to the cluster.

        Providers should implement this with the following signature:

            add_slaves(self, *, user: str, identity_file: str, num_slaves: int, **provider_specific_options)

        This method should be called after the new hosts are online and have been
        added to the cluster's internal list.
        """
        hosts = [self.master_ip] + self.slave_ips
        partial_func = functools.partial(
            add_slaves_node,
            java_version=self.java_version,
            services=self.services,
            user=user,
            identity_file=identity_file,
            cluster=self,
            new_hosts=new_hosts)
        run_against_hosts(partial_func=partial_func, hosts=hosts)

        master_ssh_client = get_ssh_client(
            user=user,
            host=self.master_ip,
            identity_file=identity_file)
        with master_ssh_client:
            for service in self.services:
                service.configure_master(
                    ssh_client=master_ssh_client,
                    cluster=self)

    def remove_slaves(self, *, user: str, identity_file: str):
        """
        Remove some slaves from the cluster.

        Providers should implement this method with the following signature:

            remove_slaves(self, *, user: str, identity_file: str, num_slaves: int)

        This method should be called after the provider has removed the slaves
        from the cluster's internal list but before the instances themselves
        have been terminated.

        This method simply makes sure that the rest of the cluster knows that
        the relevant slaves are no longer part of the cluster.
        """
        self.load_manifest(user=user, identity_file=identity_file)

        partial_func = functools.partial(
            remove_slaves_node,
            user=user,
            identity_file=identity_file,
            services=self.services,
            cluster=self)
        hosts = [self.master_ip] + self.slave_ips

        run_against_hosts(partial_func=partial_func, hosts=hosts)

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

        run_against_hosts(partial_func=partial_func, hosts=hosts)

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

        run_against_hosts(partial_func=partial_func, hosts=hosts)

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


def generate_template_mapping(
    *,
    cluster: FlintrockCluster,
    # If we add additional services later on we may want to refactor
    # this to take a list of services and dynamically pull the service
    # name.
    spark_executor_instances: int,
    hadoop_version: str,
    spark_version: str
) -> dict:
    """
    Generate a template mapping from a FlintrockCluster instance that we can use
    to fill in template parameters.
    """
    hadoop_root_dir = posixpath.join(cluster.storage_dirs.root, 'hadoop')
    hadoop_ephemeral_dirs = ','.join(
        posixpath.join(path, 'hadoop')
        for path in cluster.storage_dirs.ephemeral
    )
    spark_root_dir = posixpath.join(cluster.storage_dirs.root, 'spark')
    spark_ephemeral_dirs = ','.join(
        posixpath.join(path, 'spark')
        for path in cluster.storage_dirs.ephemeral
    )

    template_mapping = {
        'master_ip': cluster.master_ip,
        'master_host': cluster.master_host,
        'master_private_host': cluster.master_private_host,
        'slave_ips': '\n'.join(cluster.slave_ips),
        'slave_hosts': '\n'.join(cluster.slave_hosts),
        'slave_private_hosts': '\n'.join(cluster.slave_private_hosts),

        'hadoop_version': hadoop_version,
        'hadoop_short_version': '.'.join(hadoop_version.split('.')[:2]),
        'spark_version': spark_version,
        'spark_short_version': '.'.join(spark_version.split('.')[:2]) if '.' in spark_version else spark_version,

        'spark_executor_instances': spark_executor_instances,

        'hadoop_root_dir': hadoop_root_dir,
        'hadoop_ephemeral_dirs': hadoop_ephemeral_dirs,
        'spark_root_dir': spark_root_dir,
        'spark_ephemeral_dirs': spark_ephemeral_dirs,

        # If ephemeral storage is available, it replaces the root volume, which is
        # typically persistent. We don't want to mix persistent and ephemeral
        # storage since that causes problems after cluster stop/start; some volumes
        # have leftover data, whereas others start fresh.
        'hadoop_root_ephemeral_dirs': hadoop_ephemeral_dirs if hadoop_ephemeral_dirs else hadoop_root_dir,
        'spark_root_ephemeral_dirs': spark_ephemeral_dirs if spark_ephemeral_dirs else spark_root_dir,
    }

    return template_mapping


# TODO: Cache these files. (?) They are being read potentially tens or
#       hundreds of times. Maybe it doesn't matter because the files
#       are so small.
def get_formatted_template(*, path: str, mapping: dict) -> str:
    with open(path) as f:
        formatted = f.read().format(**mapping)
    return formatted


def run_against_hosts(*, partial_func: functools.partial, hosts: list):
    """
    Run a function asynchronously against each of the provided hosts.

    This function assumes that partial_func accepts `host` as a keyword argument.
    """
    with concurrent.futures.ThreadPoolExecutor(len(hosts)) as executor:
        futures = {
            executor.submit(functools.partial(partial_func, host=host))
            for host in hosts
        }
        concurrent.futures.wait(futures, return_when=FIRST_EXCEPTION)
        for future in futures:
            future.result()


def get_installed_java_version(client: paramiko.client.SSHClient):
    """
    :return: the major version (5,6,7,8...) of the currently installed Java or None if not installed
    """
    possible_cmds = [
        "$JAVA_HOME/bin/java -version",
        "java -version"
    ]

    for command in possible_cmds:
        try:
            output = ssh_check_output(
                client=client,
                command=command)
            tokens = output.split()
            # First line of the output is like: 'openjdk version "1.8.0_252"' or 'openjdk version "11.0.7" 2020-04-14'
            # Get the version string and strip out the first two parts of the
            # version as an int: 7, 8, 9, 10...
            if len(tokens) >= 3:
                version_parts = tokens[2].strip('"').split(".")
                if len(version_parts) >= 2:
                    if version_parts[0] == "1":
                        # Java 6, 7 or 8
                        return int(version_parts[1])
                    else:
                        # Java 9+
                        return int(version_parts[0])
        except SSHError:
            pass

    return None


def ensure_java(client: paramiko.client.SSHClient, java_version: int):
    """
    Ensures that Java is available on the machine and that it has a
    version of at least java_version.

    The specified version of Java will be installed if it does not
    exist or the existing version has a major version lower than java_version.

    :param client:
    :param java_version:
        minimum version of Java required
    :return:
    """
    host = client.get_transport().getpeername()[0]
    installed_java_version = get_installed_java_version(client)

    if installed_java_version == java_version:
        logger.info("Java {j} is already installed, skipping Java install".format(j=installed_java_version))
        return

    if installed_java_version and installed_java_version > java_version:
        logger.warning("""
            Existing Java {j} installation is newer than the configured version {java_version}.
            Your applications will be executed with Java {j}.
            Please choose a different AMI if this does not work for you.
            """.format(j=installed_java_version, java_version=java_version))
        return

    if installed_java_version and installed_java_version < java_version:
        logger.info("""
                Existing Java {j} will be upgraded to AdoptOpenJDK {java_version}
                """.format(j=installed_java_version, java_version=java_version))

    # We will install AdoptOpenJDK because it gives us access to Java 8 through 15
    # Right now, Amazon Extras only provides Corretto Java 8, 11 and 15
    logger.info("[{h}] Installing AdoptOpenJDK Java {j}...".format(h=host, j=java_version))

    install_adoptopenjdk_repo(client)
    java_package = "adoptopenjdk-{j}-hotspot".format(j=java_version)
    ssh_check_output(
        client=client,
        command="""
            set -e

            # Install Java first to protect packages that depend on Java from being removed.
            sudo yum install -q -y {jp}

            # Remove any older versions of Java to force the default Java to the requested version.
            # We don't use /etc/alternatives because it does not seem to update links in /usr/lib/jvm correctly,
            # and we don't just rely on JAVA_HOME because some programs use java directly in the PATH.
            sudo yum remove -y java-1.6.0-openjdk java-1.7.0-openjdk

            sudo sh -c "echo export JAVA_HOME=/usr/lib/jvm/{jp} >> /etc/environment"
            source /etc/environment
        """.format(jp=java_package))


def install_adoptopenjdk_repo(client):
    """
    Installs the adoptopenjdk.repo file into /etc/yum.repos.d/
    """
    with client.open_sftp() as sftp:
        sftp.put(
            localpath=os.path.join(SCRIPTS_DIR, 'adoptopenjdk.repo'),
            remotepath='/tmp/adoptopenjdk.repo')
    ssh_check_output(
        client=client,
        command="""
            # Use sudo to install the repo file
            sudo mv /tmp/adoptopenjdk.repo /etc/yum.repos.d/
        """
    )


def setup_node(
        *,
        # Change this to take host, user, and identity_file?
        # Add some kind of caching for SSH connections so that they
        # can be looked up by host and reused?
        ssh_client: paramiko.client.SSHClient,
        services: list,
        java_version: int,
        cluster: FlintrockCluster):
    """
    Setup a new node.

    Cluster methods like provision_node() and add_slaves_node() should
    delegate the main work of setting up new nodes to this function.
    """
    host = ssh_client.get_transport().getpeername()[0]
    ssh_check_output(
        client=ssh_client,
        command="""
            set -e

            echo {private_key} > "$HOME/.ssh/id_rsa"
            echo {public_key} >> "$HOME/.ssh/authorized_keys"

            chmod 400 "$HOME/.ssh/id_rsa"
        """.format(
            private_key=shlex.quote(cluster.ssh_key_pair.private),
            public_key=shlex.quote(cluster.ssh_key_pair.public)))

    with ssh_client.open_sftp() as sftp:
        sftp.put(
            localpath=os.path.join(SCRIPTS_DIR, 'setup-ephemeral-storage.py'),
            remotepath='/tmp/setup-ephemeral-storage.py')

    logger.info("[{h}] Configuring ephemeral storage...".format(h=host))
    # TODO: Print some kind of warning if storage is large, since formatting
    #       will take several minutes (~4 minutes for 2TB).
    storage_dirs_raw = ssh_check_output(
        client=ssh_client,
        command="""
            set -e
            python /tmp/setup-ephemeral-storage.py
            rm -f /tmp/setup-ephemeral-storage.py
        """)
    storage_dirs = json.loads(storage_dirs_raw)

    cluster.storage_dirs.root = storage_dirs['root']
    cluster.storage_dirs.ephemeral = storage_dirs['ephemeral']

    # TODO: Move Python and Java setup to new service under services.py.
    #       New service to cover Python/Scala/Java: LanguageRuntimes (name?)
    ssh_check_output(
        client=ssh_client,
        command=(
            """
            set -e
            sudo yum install -y python3
            """
        )
    )
    ensure_java(ssh_client, java_version)

    for service in services:
        try:
            service.install(
                ssh_client=ssh_client,
                cluster=cluster,
            )
        except Exception as e:
            raise Exception(
                "Failed to install {}."
                .format(type(service).__name__)
            ) from e


def provision_cluster(
        *,
        cluster: FlintrockCluster,
        java_version: int,
        services: list,
        user: str,
        identity_file: str):
    """
    Connect to a freshly launched cluster and install the specified services.
    """
    partial_func = functools.partial(
        provision_node,
        java_version=java_version,
        services=services,
        user=user,
        identity_file=identity_file,
        cluster=cluster)
    hosts = [cluster.master_ip] + cluster.slave_ips

    run_against_hosts(partial_func=partial_func, hosts=hosts)

    master_ssh_client = get_ssh_client(
        user=user,
        host=cluster.master_ip,
        identity_file=identity_file)

    with master_ssh_client:
        manifest = {
            'java_version': java_version,
            'services': [[type(m).__name__, m.manifest] for m in services],
            'ssh_key_pair': cluster.ssh_key_pair._asdict(),
        }
        # The manifest tells us how the cluster is configured. We'll need this
        # when we resize the cluster or restart it.
        ssh_check_output(
            client=master_ssh_client,
            command="""
                echo {m} > "$HOME/.flintrock-manifest.json"
                chmod go-rw "$HOME/.flintrock-manifest.json"
            """.format(
                m=shlex.quote(json.dumps(manifest, indent=4, sort_keys=True))
            ))

        for service in services:
            service.configure_master(
                ssh_client=master_ssh_client,
                cluster=cluster)

    for service in services:
        service.health_check(master_host=cluster.master_ip)


def provision_node(
        *,
        java_version: int,
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
        setup_node(
            ssh_client=client,
            services=services,
            java_version=java_version,
            cluster=cluster)
        for service in services:
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


def add_slaves_node(
        *,
        user: str,
        host: str,
        identity_file: str,
        java_version: int,
        services: list,
        cluster: FlintrockCluster,
        new_hosts: list):
    """
    If the node is new, set it up. If not, just reconfigure it to recognize
    the newly added nodes.

    This method is role-agnostic; it runs on both the cluster master and slaves.
    This method is meant to be called asynchronously.
    """
    is_new_host = host in new_hosts

    client = get_ssh_client(
        user=user,
        host=host,
        identity_file=identity_file,
        wait=is_new_host)

    with client:
        if is_new_host:
            setup_node(
                ssh_client=client,
                services=services,
                java_version=java_version,
                cluster=cluster)

        for service in services:
            service.configure(
                ssh_client=client,
                cluster=cluster)


def remove_slaves_node(
        *,
        user: str,
        host: str,
        identity_file: str,
        services: list,
        cluster: FlintrockCluster):
    """
    Update the services on a node to remove the provided slaves.

    This method is role-agnostic; it runs on both the cluster master and slaves.
    This method is meant to be called asynchronously.
    """
    ssh_client = get_ssh_client(
        user=user,
        host=host,
        identity_file=identity_file)

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

    logger.info("[{h}] Running command...".format(h=host))

    command_str = ' '.join(command)

    with ssh_client:
        ssh_check_output(
            client=ssh_client,
            command=command_str)

    logger.info("[{h}] Command complete.".format(h=host))


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
            logger.info("[{h}] Copying file...".format(h=host))

            sftp.put(localpath=local_path, remotepath=remote_path)

            logger.info("[{h}] Copy complete.".format(h=host))


# This is necessary down here since we have a circular import dependency between
# core.py and services.py. I've thought about how to remove this circular dependency,
# but for now this seems like what we need to go with.
# Flintrock modules
from .services import HDFS, Spark  # Used by start_cluster() # noqa
