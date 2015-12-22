import json
import os
import posixpath
import shlex
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.request
from collections import namedtuple

# External modules
import paramiko

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

ClusterInfo = namedtuple(
    'ClusterInfo', [
        'name',
        'ssh_key_pair',
        'user',
        'master_host',
        'slave_hosts',
        'storage_dirs',
    ])


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


def cluster_info_to_template_mapping(
        *,
        cluster_info: ClusterInfo,
        module: str) -> dict:
    """
    Convert a ClusterInfo tuple to a dictionary that we can use to fill in template
    parameters.
    """
    template_mapping = {}

    for k, v in cluster_info._asdict().items():
        if k == 'slave_hosts':
            template_mapping.update({k: '\n'.join(v)})
        elif k == 'storage_dirs':
            template_mapping.update({
                'root_dir': v['root'] + '/' + module,
                'ephemeral_dirs': ','.join(path + '/' + module for path in v['ephemeral'])})

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


# TODO: Cache these files. (?) They are being read potentially tens or
#       hundreds of times. Maybe it doesn't matter because the files
#       are so small.
# NOTE: functools.lru_cache() doesn't work here because the mapping is
#       not hashable.
def get_formatted_template(path: str, mapping: dict) -> str:
    with open(path) as f:
        formatted = f.read().format(**mapping)

    return formatted


class HDFS:
    def __init__(self, version):
        self.version = version

    def install(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster_info: ClusterInfo):
        print("[{h}] Installing HDFS...".format(
            h=ssh_client.get_transport().getpeername()[0]))

        with ssh_client.open_sftp() as sftp:
            sftp.put(
                localpath=os.path.join(THIS_DIR, 'download-hadoop.py'),
                remotepath='/tmp/download-hadoop.py')

        ssh_check_output(
            client=ssh_client,
            command="""
                set -e

                python /tmp/download-hadoop.py "{version}"

                mkdir "hadoop"
                mkdir "hadoop/conf"

                tar xzf "hadoop-{version}.tar.gz" -C "hadoop" --strip-components=1
                rm "hadoop-{version}.tar.gz"
            """.format(version=self.version))

    def configure(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster_info: ClusterInfo):
        # TODO: os.walk() through these files.
        template_paths = [
            'hadoop/conf/masters',
            'hadoop/conf/slaves',
            'hadoop/conf/hadoop-env.sh',
            'hadoop/conf/core-site.xml',
            'hadoop/conf/hdfs-site.xml']

        for template_path in template_paths:
            ssh_check_output(
                client=ssh_client,
                command="""
                    echo {f} > {p}
                """.format(
                    f=shlex.quote(
                        get_formatted_template(
                            path=os.path.join(THIS_DIR, "templates", template_path),
                            mapping=cluster_info_to_template_mapping(
                                cluster_info=cluster_info,
                                module='hdfs'))),
                    p=shlex.quote(template_path)))

    # TODO: Convert this into start_master() and split master- or slave-specific
    #       stuff out of configure() into configure_master() and configure_slave().
    def configure_master(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster_info: ClusterInfo):
        host = ssh_client.get_transport().getpeername()[0]
        print("[{h}] Configuring HDFS master...".format(h=host))

        ssh_check_output(
            client=ssh_client,
            command="""
                ./hadoop/bin/hdfs namenode -format -nonInteractive
                ./hadoop/sbin/start-dfs.sh
            """)

    def configure_slave(self):
        pass

    def health_check(self, master_host: str):
        """
        Check that HDFS is functioning.
        """
        # This info is not helpful as a detailed health check, but it gives us
        # an up / not up signal.
        hdfs_master_ui = 'http://{m}:50070/webhdfs/v1/?op=GETCONTENTSUMMARY'.format(m=master_host)

        try:
            hdfs_ui_info = json.loads(
                urllib.request.urlopen(hdfs_master_ui).read().decode('utf-8'))
        except Exception as e:
            # TODO: Catch a more specific problem.
            print("HDFS health check failed.", file=sys.stderr)
            raise

        print("HDFS online.")


# TODO: Turn this into an implementation of an abstract FlintrockModule class. (?)
class Spark:
    def __init__(self, version: str=None, git_commit: str=None, git_repository: str=None):
        # TODO: Convert these checks into something that throws a proper exception.
        #       Perhaps reuse logic from CLI.
        assert bool(version) ^ bool(git_commit)
        if git_commit:
            assert git_repository

        self.version = version
        self.git_commit = git_commit
        self.git_repository = git_repository

    def install(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster_info: ClusterInfo):
        """
        Downloads and installs Spark on a given node.
        """
        # TODO: Allow users to specify the Spark "distribution". (?)
        distribution = 'hadoop2.6'

        print("[{h}] Installing Spark...".format(
            h=ssh_client.get_transport().getpeername()[0]))

        try:
            if self.version:
                with ssh_client.open_sftp() as sftp:
                    sftp.put(
                        localpath=os.path.join(THIS_DIR, 'install-spark.sh'),
                        remotepath='/tmp/install-spark.sh')
                    sftp.chmod(path='/tmp/install-spark.sh', mode=0o755)
                ssh_check_output(
                    client=ssh_client,
                    command="""
                        set -e
                        /tmp/install-spark.sh {spark_version} {distribution}
                        rm -f /tmp/install-spark.sh
                    """.format(
                            spark_version=shlex.quote(self.version),
                            distribution=shlex.quote(distribution)))
            else:
                ssh_check_output(
                    client=ssh_client,
                    command="""
                        set -e
                        sudo yum install -y git
                        sudo yum install -y java-devel
                        """)
                ssh_check_output(
                    client=ssh_client,
                    command="""
                        set -e
                        git clone {repo} spark
                        cd spark
                        git reset --hard {commit}
                        ./make-distribution.sh -T 1C -Phadoop-2.6
                    """.format(
                        repo=shlex.quote(self.git_repository),
                        commit=shlex.quote(self.git_commit)))
        except Exception as e:
            # TODO: This should be a more specific exception.
            print("Error: Failed to install Spark.", file=sys.stderr)
            print(e, file=sys.stderr)
            raise

    def configure(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster_info: ClusterInfo):
        """
        Configures Spark after it's installed.

        This method is master/slave-agnostic.
        """
        template_paths = [
            'spark/conf/spark-env.sh',
            'spark/conf/slaves']
        for template_path in template_paths:
            ssh_check_output(
                client=ssh_client,
                command="""
                    echo {f} > {p}
                """.format(
                    f=shlex.quote(
                        get_formatted_template(
                            path=os.path.join(THIS_DIR, "templates", template_path),
                            mapping=cluster_info_to_template_mapping(
                                cluster_info=cluster_info,
                                module='spark'))),
                    p=shlex.quote(template_path)))

    # TODO: Convert this into start_master() and split master- or slave-specific
    #       stuff out of configure() into configure_master() and configure_slave().
    #       start_slave() can block until slave is fully up; that way we don't need
    #       a sleep() before starting the master.
    def configure_master(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster_info: ClusterInfo):
        """
        Configures the Spark master and starts both the master and slaves.
        """
        host = ssh_client.get_transport().getpeername()[0]
        print("[{h}] Configuring Spark master...".format(h=host))

        # TODO: Maybe move this shell script out to some separate file/folder
        #       for the Spark module.
        # TODO: Add some timeout for waiting on master UI to come up.
        ssh_check_output(
            client=ssh_client,
            command="""
                set -e

                spark/sbin/start-master.sh

                set +e

                master_ui_response_code=0
                while [ "$master_ui_response_code" -ne 200 ]; do
                    sleep 1
                    master_ui_response_code="$(
                        curl --head --silent --output /dev/null \
                             --write-out "%{{http_code}}" {m}:8080
                    )"
                done

                set -e

                spark/sbin/start-slaves.sh
            """.format(
                m=shlex.quote(cluster_info.master_host)))

    def configure_slave(self):
        pass

    def health_check(self, master_host: str):
        """
        Check that Spark is functioning.
        """
        spark_master_ui = 'http://{m}:8080/json/'.format(m=master_host)

        try:
            spark_ui_info = json.loads(
                urllib.request.urlopen(spark_master_ui).read().decode('utf-8'))
        except Exception as e:
            # TODO: Catch a more specific problem known to be related to Spark not
            #       being up; provide a slightly better error message, and don't
            #       dump a large stack trace on the user.
            print("Spark health check failed.", file=sys.stderr)
            raise

        print(textwrap.dedent(
            """\
            Spark Health Report:
              * Master: {status}
              * Workers: {workers}
              * Cores: {cores}
              * Memory: {memory:.1f} GB\
            """.format(
                status=spark_ui_info['status'],
                workers=len(spark_ui_info['workers']),
                cores=spark_ui_info['cores'],
                memory=spark_ui_info['memory'] / 1024)))


def get_ssh_client(
        *,
        user: str,
        host: str,
        identity_file: str,
        print_status: bool=False) -> paramiko.client.SSHClient:
    """
    Get an SSH client for the provided host, waiting as necessary for SSH to become available.
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


def provision_node(
        *,
        modules: list,
        user: str,
        host: str,
        identity_file: str,
        cluster_info: ClusterInfo):
    """
    Connect to a freshly launched node, set it up for SSH access, and
    install the specified modules.

    This function is intended to be called on all cluster nodes in parallel.

    No master- or slave-specific logic should be in this method.
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
                private_key=shlex.quote(cluster_info.ssh_key_pair.private),
                public_key=shlex.quote(cluster_info.ssh_key_pair.public)))

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

        cluster_info.storage_dirs['root'] = storage_dirs['root']
        cluster_info.storage_dirs['ephemeral'] = storage_dirs['ephemeral']

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

        for module in modules:
            module.install(
                ssh_client=client,
                cluster_info=cluster_info)
            module.configure(
                ssh_client=client,
                cluster_info=cluster_info)


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


def start_node(
        *,
        modules: list,
        user: str,
        host: str,
        identity_file: str,
        cluster_info: ClusterInfo):
    """
    Connect to an existing node that has just been started up again and prepare it for
    work.
    """
    ssh_client = get_ssh_client(
        user=user,
        host=host,
        identity_file=identity_file,
        print_status=True)

    with ssh_client:
        # TODO: Consider consolidating ephemeral storage code under a dedicated
        #       Flintrock module.
        if cluster_info.storage_dirs['ephemeral']:
            ssh_check_output(
                client=ssh_client,
                command="""
                    sudo chown "{u}:{u}" {d}
                """.format(
                    u=user,
                    d=' '.join(cluster_info.storage_dirs['ephemeral'])))

        for module in modules:
            module.configure(
                ssh_client=ssh_client,
                cluster_info=cluster_info)


def run_command_node(*, user: str, host: str, identity_file: str, command: tuple):
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


def copy_file_node(*, user: str, host: str, identity_file: str, local_path: str, remote_path: str):
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
            raise Exception("Remote directory does not exist: {d}".format(d=remote_dir))

        with ssh_client.open_sftp() as sftp:
            print("[{h}] Copying file...".format(h=host))

            sftp.put(localpath=local_path, remotepath=remote_path)

            print("[{h}] Copy complete.".format(h=host))
