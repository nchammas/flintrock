import json
import os
import shlex
import sys
import textwrap
import urllib.request

# External modules
import paramiko

# Flintrock modules
from .core import (
    FlintrockCluster,
    generate_template_mapping,
    get_formatted_template,
)
from .ssh import ssh_check_output

FROZEN = getattr(sys, 'frozen', False)

if FROZEN:
    THIS_DIR = sys._MEIPASS
else:
    THIS_DIR = os.path.dirname(os.path.realpath(__file__))

SCRIPTS_DIR = os.path.join(THIS_DIR, 'scripts')


class FlintrockService:
    """
    This is an abstract class. Implementations of this class capture all the logic
    required to fully install and manage services like Spark on Flintrock clusters.
    """

    def __init__(self):
        """
        This is the only method signature that implementations don't have to follow.
        Use this method to set properties like the service version or download source
        which the rest of the methods here will need to do their work.
        """
        raise NotImplementedError

    def install(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        """
        Install the service on a node via the provided SSH client. This typically
        means downloading a software package and maybe even building it if necessary.

        This method is role-agnostic; it runs on both the cluster master and slaves.
        This method is meant to be called asynchronously.
        """
        raise NotImplementedError

    def configure(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        """
        Configure the installed service on a node via the provided SSH client. This
        typically means using templates to create configuration files on the node.

        This method is role-agnostic; it runs on both the cluster master and slaves.
        This method is meant to be called asynchronously.
        """
        raise NotImplementedError

    def configure_master(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        """
        Configure the service master on a node via the provided SSH client after the
        role-agnostic configuration in configure() is complete. Start the master and
        slaves.

        This method is meant to be called once on the cluster master.
        This method is meant to be called asynchronously.
        """
        raise NotImplementedError

    def configure_slave(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        """
        Configure a service slave on a node via the provided SSH client after the
        role-agnostic configuration in configure() is complete.

        This method is meant to be called once on each cluster slave.
        This method is meant to be called asynchronously.
        """
        raise NotImplementedError

    def health_check(
            self,
            master_host: str):
        """
        Check that the service is up and running by querying the cluster master.
        """
        raise NotImplementedError


class HDFS(FlintrockService):
    def __init__(self, *, version, download_source):
        self.version = version
        self.download_source = download_source
        self.manifest = {'version': version, 'download_source': download_source}

    def install(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        print("[{h}] Installing HDFS...".format(
            h=ssh_client.get_transport().getpeername()[0]))

        with ssh_client.open_sftp() as sftp:
            sftp.put(
                localpath=os.path.join(SCRIPTS_DIR, 'download-hadoop.py'),
                remotepath='/tmp/download-hadoop.py')

        ssh_check_output(
            client=ssh_client,
            command="""
                set -e

                python /tmp/download-hadoop.py "{version}" "{download_source}"

                mkdir "hadoop"
                mkdir "hadoop/conf"

                tar xzf "hadoop-{version}.tar.gz" -C "hadoop" --strip-components=1
                rm "hadoop-{version}.tar.gz"

                for f in $(find hadoop/bin -type f -executable -not -name '*.cmd'); do
                    sudo ln -s "$(pwd)/$f" "/usr/local/bin/$(basename $f)"
                done
                echo "export HADOOP_LIBEXEC_DIR='$(pwd)/hadoop/libexec'" >> .bashrc
            """.format(version=self.version, download_source=self.download_source))

    def configure(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        # TODO: os.walk() through these files.
        template_paths = [
            'hadoop/conf/masters',
            'hadoop/conf/slaves',
            'hadoop/conf/hadoop-env.sh',
            'hadoop/conf/core-site.xml',
            'hadoop/conf/hdfs-site.xml',
        ]

        for template_path in template_paths:
            ssh_check_output(
                client=ssh_client,
                command="""
                    echo {f} > {p}
                """.format(
                    f=shlex.quote(
                        get_formatted_template(
                            path=os.path.join(THIS_DIR, "templates", template_path),
                            mapping=generate_template_mapping(
                                cluster=cluster,
                                hadoop_version=self.version,
                                # Hadoop doesn't need to know what
                                # Spark version we're using.
                                spark_version='',
                            ))),
                    p=shlex.quote(template_path)))

    # TODO: Convert this into start_master() and split master- or slave-specific
    #       stuff out of configure() into configure_master() and configure_slave().
    def configure_master(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        host = ssh_client.get_transport().getpeername()[0]
        print("[{h}] Configuring HDFS master...".format(h=host))

        ssh_check_output(
            client=ssh_client,
            command="""
                ./hadoop/bin/hdfs namenode -format -nonInteractive
                ./hadoop/sbin/start-dfs.sh
            """)

    def health_check(self, master_host: str):
        # This info is not helpful as a detailed health check, but it gives us
        # an up / not up signal.
        hdfs_master_ui = 'http://{m}:50070/webhdfs/v1/?op=GETCONTENTSUMMARY'.format(m=master_host)

        try:
            hdfs_ui_info = json.loads(  # noqa
                urllib.request.urlopen(hdfs_master_ui).read().decode('utf-8'))
        except Exception as e:
            # TODO: Catch a more specific problem.
            # TODO: Don't print to screen here. Raise the exception and let the
            #       catcher decide on whether to print.
            print("HDFS health check failed.", file=sys.stderr)
            raise

        print("HDFS online.")


class Spark(FlintrockService):
    def __init__(
        self,
        *,
        version: str=None,
        hadoop_version: str,
        download_source: str=None,
        git_commit: str=None,
        git_repository: str=None
    ):
        # TODO: Convert these checks into something that throws a proper exception.
        #       Perhaps reuse logic from CLI.
        assert bool(version) ^ bool(git_commit)
        if git_commit:
            assert git_repository

        self.version = version
        self.hadoop_version = hadoop_version
        self.download_source = download_source
        self.git_commit = git_commit
        self.git_repository = git_repository

        self.manifest = {
            'version': version,
            'hadoop_version': hadoop_version,
            'download_source': download_source,
            'git_commit': git_commit,
            'git_repository': git_repository}

    def install(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):

        print("[{h}] Installing Spark...".format(
            h=ssh_client.get_transport().getpeername()[0]))

        try:
            if self.version:
                with ssh_client.open_sftp() as sftp:
                    sftp.put(
                        localpath=os.path.join(SCRIPTS_DIR, 'install-spark.sh'),
                        remotepath='/tmp/install-spark.sh')
                    sftp.chmod(path='/tmp/install-spark.sh', mode=0o755)
                url = self.download_source.format(v=self.version)
                ssh_check_output(
                    client=ssh_client,
                    command="""
                        set -e
                        /tmp/install-spark.sh {url}
                        rm -f /tmp/install-spark.sh
                    """.format(url=shlex.quote(url)))
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
                        if [ -e "make-distribution.sh" ]; then
                            ./make-distribution.sh -Phadoop-{hadoop_short_version}
                        else
                            ./dev/make-distribution.sh -Phadoop-{hadoop_short_version}
                        fi
                    """.format(
                        repo=shlex.quote(self.git_repository),
                        commit=shlex.quote(self.git_commit),
                        hadoop_short_version='.'.join(self.hadoop_version.split('.')[:2]),
                    ))
            ssh_check_output(
                client=ssh_client,
                command="""
                    set -e
                    for f in $(find spark/bin -type f -executable -not -name '*.cmd'); do
                        sudo ln -s "$(pwd)/$f" "/usr/local/bin/$(basename $f)"
                    done
                    echo "export SPARK_HOME='$(pwd)/spark'" >> .bashrc
                """)
        except Exception as e:
            # TODO: This should be a more specific exception.
            print("Error: Failed to install Spark.", file=sys.stderr)
            print(e, file=sys.stderr)
            raise

    def configure(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        template_paths = [
            'spark/conf/spark-env.sh',
            'spark/conf/slaves',
            'spark/conf/spark-defaults.conf',
        ]
        for template_path in template_paths:
            ssh_check_output(
                client=ssh_client,
                command="""
                    echo {f} > {p}
                """.format(
                    f=shlex.quote(
                        get_formatted_template(
                            path=os.path.join(THIS_DIR, "templates", template_path),
                            mapping=generate_template_mapping(
                                cluster=cluster,
                                hadoop_version=self.hadoop_version,
                                spark_version=self.version,
                            ))),
                    p=shlex.quote(template_path)))

    # TODO: Convert this into start_master() and split master- or slave-specific
    #       stuff out of configure() into configure_master() and configure_slave().
    #       start_slave() can block until slave is fully up; that way we don't need
    #       a sleep() before starting the master.
    def configure_master(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        host = ssh_client.get_transport().getpeername()[0]
        print("[{h}] Configuring Spark master...".format(h=host))

        # TODO: Maybe move this shell script out to some separate file/folder
        #       for the Spark service.
        # TODO: Add some timeout for waiting on master UI to come up.
        ssh_check_output(
            client=ssh_client,
            command="""
                spark/sbin/start-all.sh

                master_ui_response_code=0
                while [ "$master_ui_response_code" -ne 200 ]; do
                    sleep 1
                    master_ui_response_code="$(
                        curl --head --silent --output /dev/null \
                             --write-out "%{{http_code}}" {m}:8080
                    )"
                done
            """.format(
                m=shlex.quote(cluster.master_host)))

    def health_check(self, master_host: str):
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

        # TODO: Don't print here. Return this and let the caller print.
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
