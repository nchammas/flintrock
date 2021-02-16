import json
import os
import shlex
import socket
import sys
import urllib.error
import urllib.request
import logging

# External modules
import paramiko

# Flintrock modules
from .core import (
    FlintrockCluster,
    generate_template_mapping,
    get_formatted_template,
)
from .ssh import ssh_check_output
from .util import spark_hadoop_build_version

FROZEN = getattr(sys, 'frozen', False)

if FROZEN:
    THIS_DIR = sys._MEIPASS
else:
    THIS_DIR = os.path.dirname(os.path.realpath(__file__))

SCRIPTS_DIR = os.path.join(THIS_DIR, 'scripts')


logger = logging.getLogger('flintrock.services')


# TODO: Move this back to ec2.py. EC2-specific login should not live here.
class SecurityGroupRule:
    def __init__(
        self,
        ip_protocol,
        from_port,
        to_port,
        src_group=None,
        cidr_ip=None,
    ):
        if src_group and cidr_ip:
            raise ValueError(
                "src_group and cidr_ip are mutually exclusive. Specify one or the other. "
                "See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.SecurityGroup.authorize_ingress"
            )

        if not src_group and not cidr_ip:
            raise ValueError("One of src_group or cidr_ip must be specified.")

        self.ip_protocol = ip_protocol
        self.from_port = from_port
        self.to_port = to_port
        # We set the default values to empty string so calls to boto3 accept unset parameters.
        # See: https://github.com/boto/boto3/issues/331
        self.src_group = src_group if src_group else ''
        self.cidr_ip = cidr_ip if cidr_ip else ''

    def __str__(self):
        return str(vars(self))


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

    def get_security_group_rules(self, flintrock_client_cidr: str, flintrock_client_group: str):
        """
        Return the EC2 SecurityGroupRules required by this service.
        """
        raise NotImplementedError


class HDFS(FlintrockService):
    def __init__(self, *, version, download_source):
        self.version = version
        self.download_source = download_source
        self.name_node_ui_port = 50070 if version < '3.0' else 9870
        self.manifest = {'version': version, 'download_source': download_source}

    def install(
        self,
        ssh_client: paramiko.client.SSHClient,
        cluster: FlintrockCluster,
    ):
        logger.info(
            "[{h}] Installing HDFS..."
            .format(h=ssh_client.get_transport().getpeername()[0])
        )

        with ssh_client.open_sftp() as sftp:
            sftp.put(
                localpath=os.path.join(SCRIPTS_DIR, 'download-package.py'),
                remotepath='/tmp/download-package.py')

        logger.debug(
            "[{h}] Downloading Hadoop from: {s}"
            .format(
                h=ssh_client.get_transport().getpeername()[0],
                s=self.download_source,
            )
        )

        ssh_check_output(
            client=ssh_client,
            command="""
                set -e

                python /tmp/download-package.py "{download_source}" "hadoop"

                for f in $(find hadoop/bin -type f -executable -not -name '*.cmd'); do
                    sudo ln -s "$(pwd)/$f" "/usr/local/bin/$(basename $f)"
                done

                echo "export HADOOP_LIBEXEC_DIR='$(pwd)/hadoop/libexec'" >> .bashrc
            """.format(
                # version=self.version,
                download_source=self.download_source.format(v=self.version),
            ))

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

        ssh_check_output(
            client=ssh_client,
            command="mkdir -p hadoop/conf",
        )

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
                                spark_executor_instances=0,
                            ))),
                    p=shlex.quote(template_path)))

    # TODO: Convert this into start_master() and split master- or slave-specific
    #       stuff out of configure() into configure_master() and configure_slave().
    def configure_master(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):
        host = ssh_client.get_transport().getpeername()[0]
        logger.info("[{h}] Configuring HDFS master...".format(h=host))

        ssh_check_output(
            client=ssh_client,
            command="""
                # `|| true` because on cluster restart this command will fail.
                ./hadoop/bin/hdfs namenode -format -nonInteractive || true
            """)

        # This loop is a band-aid for: https://github.com/nchammas/flintrock/issues/157
        attempt_limit = 3
        for attempt in range(attempt_limit):
            try:
                ssh_check_output(
                    client=ssh_client,
                    command="""
                        ./hadoop/sbin/stop-dfs.sh
                        ./hadoop/sbin/start-dfs.sh

                        master_ui_response_code=0
                        while [ "$master_ui_response_code" -ne 200 ]; do
                            sleep 1
                            master_ui_response_code="$(
                                curl \
                                    --location --head --silent \
                                    --output /dev/null \
                                    --write-out "%{{http_code}}" \
                                    {m}:{p}
                            )"
                        done
                    """.format(m=shlex.quote(cluster.master_private_host), p=self.name_node_ui_port),
                    timeout_seconds=90
                )
                break
            except socket.timeout as e:
                logger.debug(
                    "Timed out waiting for HDFS master to come up.{}"
                    .format(" Trying again..." if attempt < attempt_limit - 1 else "")
                )
        else:
            raise Exception("Time out waiting for HDFS master to come up.")

    def health_check(self, master_host: str):
        # This info is not helpful as a detailed health check, but it gives us
        # an up / not up signal.
        hdfs_master_ui = 'http://{m}:{p}/webhdfs/v1/?op=GETCONTENTSUMMARY'.format(m=master_host, p=self.name_node_ui_port)

        try:
            json.loads(
                urllib.request
                .urlopen(hdfs_master_ui)
                .read()
                .decode('utf-8'))
            logger.info("HDFS online.")
        except Exception as e:
            raise Exception("HDFS health check failed.") from e

    def get_security_group_rules(self, flintrock_client_cidr: str=None, flintrock_client_group: str=None):
        return [
            SecurityGroupRule(
                ip_protocol='tcp',
                from_port=self.name_node_ui_port,
                to_port=self.name_node_ui_port,
                cidr_ip=flintrock_client_cidr,
                src_group=flintrock_client_group,
            )
        ]


class Spark(FlintrockService):
    def __init__(
        self,
        *,
        spark_executor_instances: int,
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

        self.spark_executor_instances = spark_executor_instances
        self.version = version
        self.hadoop_version = hadoop_version
        self.download_source = download_source
        self.git_commit = git_commit
        self.git_repository = git_repository

        self.manifest = {
            'version': version,
            'spark_executor_instances': spark_executor_instances,
            'hadoop_version': hadoop_version,
            'download_source': download_source,
            'git_commit': git_commit,
            'git_repository': git_repository}

    def install(
        self,
        ssh_client: paramiko.client.SSHClient,
        cluster: FlintrockCluster,
    ):
        logger.info(
            "[{h}] Installing Spark..."
            .format(h=ssh_client.get_transport().getpeername()[0])
        )

        if self.version:
            with ssh_client.open_sftp() as sftp:
                sftp.put(
                    localpath=os.path.join(SCRIPTS_DIR, 'download-package.py'),
                    remotepath='/tmp/download-package.py')

            logger.debug(
                "[{h}] Downloading Spark from: {s}"
                .format(
                    h=ssh_client.get_transport().getpeername()[0],
                    s=self.download_source,
                )
            )

            ssh_check_output(
                client=ssh_client,
                command="""
                    python /tmp/download-package.py "{download_source}" "spark"
                """.format(
                    # version=self.version,
                    download_source=self.download_source.format(v=self.version),
                ))
        else:
            ssh_check_output(
                client=ssh_client,
                command="""
                    set -e
                    sudo yum install -y git
                    sudo yum install -y java-devel
                    """)

            logger.debug(
                "[{h}] Cloning Spark at {c} from: {s}"
                .format(
                    h=ssh_client.get_transport().getpeername()[0],
                    c=self.git_commit,
                    s=self.git_repository,
                )
            )

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
                    hadoop_short_version=spark_hadoop_build_version(self.hadoop_version),
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

    def configure(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster: FlintrockCluster):

        template_paths = [
            'spark/conf/spark-env.sh',
            'spark/conf/slaves',
        ]

        ssh_check_output(
            client=ssh_client,
            command="mkdir -p spark/conf",
        )

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
                                spark_executor_instances=self.spark_executor_instances,
                                hadoop_version=self.hadoop_version,
                                spark_version=self.version or self.git_commit,
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
        logger.info("[{h}] Configuring Spark master...".format(h=host))

        # This loop is a band-aid for: https://github.com/nchammas/flintrock/issues/129
        attempt_limit = 3
        for attempt in range(attempt_limit):
            try:
                ssh_check_output(
                    client=ssh_client,
                    # Maybe move this shell script out to some separate
                    # file/folder for the Spark service.
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
                    """.format(m=shlex.quote(cluster.master_private_host)),
                    timeout_seconds=90
                )
                break
            except socket.timeout as e:
                logger.debug(
                    "Timed out waiting for Spark master to come up.{}"
                    .format(" Trying again..." if attempt < attempt_limit - 1 else "")
                )
        else:
            raise Exception("Timed out waiting for Spark master to come up.")

    def health_check(self, master_host: str):
        spark_master_ui = 'http://{m}:8080/json/'.format(m=master_host)

        try:
            json.loads(
                urllib.request
                .urlopen(spark_master_ui)
                .read()
                .decode('utf-8')
            )
            # TODO: Don't print here. Return this and let the caller print.
            logger.info("Spark online.")
        except Exception as e:
            # TODO: Catch a more specific problem known to be related to Spark not
            #       being up; provide a slightly better error message, and don't
            #       dump a large stack trace on the user.
            raise Exception("Spark health check failed.") from e

    def get_security_group_rules(self, flintrock_client_cidr: str=None, flintrock_client_group: str=None):
        return [
            SecurityGroupRule(
                ip_protocol='tcp',
                from_port=8080,
                to_port=8081,
                cidr_ip=flintrock_client_cidr,
                src_group=flintrock_client_group,
            ),
            SecurityGroupRule(
                ip_protocol='tcp',
                from_port=4040,
                to_port=4050,
                cidr_ip=flintrock_client_cidr,
                src_group=flintrock_client_group,
            ),
            SecurityGroupRule(
                ip_protocol='tcp',
                from_port=7077,
                to_port=7077,
                cidr_ip=flintrock_client_cidr,
                src_group=flintrock_client_group,
            ),
            # Spark REST Server
            SecurityGroupRule(
                ip_protocol='tcp',
                from_port=6066,
                to_port=6066,
                cidr_ip=flintrock_client_cidr,
                src_group=flintrock_client_group,
            ),
        ]
