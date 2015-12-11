"""
Flintrock

A command-line tool and library for launching Apache Spark clusters.

Major TODOs:
    * "Fix" Hadoop 2.6 S3 setup by installing appropriate Hadoop libraries
      See: https://issues.apache.org/jira/browse/SPARK-7442
    * ClusterInfo namedtuple -> FlintrockCluster class
        - Platform-specific (e.g. EC2) implementations of class add methods to
          stop, start, describe (with YAML output) etc. clusters
        - Implement method that takes cluster name and returns FlintrockCluster
    * Support submit command for Spark applications. Like a wrapper around spark-submit. (?)
    * Check that EC2 enhanced networking is enabled.

Other TODOs:
    * Use IAM roles to launch instead of AWS keys.
    * Setup and teardown VPC, routes, gateway, etc. from scratch.
    * Use SSHAgent instead of .pem files (?).
    * Automatically replace failed instances during launch, perhaps up to a
      certain limit (1-2 instances).
    * Upgrade check -- Is a newer version of Flintrock available on PyPI?
    * Credits command, for crediting contributors. (?)

Distant future:
    * Local provider
"""

import os
import posixpath
import errno
import string
import sys
import shlex
import shutil
import subprocess
import pprint
import asyncio
import functools
import itertools
import socket
import json
import time
import urllib.request
import tempfile
import textwrap
from datetime import datetime
from collections import namedtuple

# External modules.
import boto
import boto.ec2
import click
import paramiko
import yaml

from flintrock import __version__

THIS_DIR = os.path.dirname(os.path.realpath(__file__))


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now().replace(microsecond=0)
        res = func(*args, **kwargs)
        end = datetime.now().replace(microsecond=0)
        print("{f} finished in {t}.".format(f=func.__name__, t=(end - start)))
        return res
    return wrapper


def get_config_file() -> str:
    """
    Get the path to Flintrock's default configuration file.
    """
    config_dir = click.get_app_dir(app_name='Flintrock')
    config_file = os.path.join(config_dir, 'config.yaml')
    return config_file


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


# TODO: Think about extending this to represent everything that defines a cluster.
#           * name
#           * installed modules (?)
#           * etc.
#
#       Convert it into a class with variations (implementations?) for the specific
#       providers.
#
#       Add class methods to start, stop, destroy, and describe clusters.
ClusterInfo = namedtuple(
    'ClusterInfo', [
        'name',
        'ssh_key_pair',
        'user',
        'master_host',
        'slave_hosts',
        'storage_dirs',
    ])


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
                localpath=os.path.join(THIS_DIR, 'get-best-apache-mirror.py'),
                remotepath='/tmp/get-best-apache-mirror.py')

        ssh_check_output(
            client=ssh_client,
            command="""
                set -e

                curl --silent --remote-name "$(
                    python /tmp/get-best-apache-mirror.py "http://www.apache.org/dyn/closer.lua/hadoop/common/hadoop-{version}/hadoop-{version}.tar.gz?as_json")"

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


@click.group()
@click.option('--config', default=get_config_file())
@click.option('--provider', default='ec2', type=click.Choice(['ec2']))
@click.version_option(version=__version__)
@click.pass_context
def cli(cli_context, config, provider):
    """
    Flintrock

    A command-line tool and library for launching Apache Spark clusters.
    """
    cli_context.obj['provider'] = provider

    if os.path.isfile(config):
        with open(config) as f:
            config_raw = yaml.safe_load(f)
            config_map = config_to_click(normalize_keys(config_raw))

        cli_context.default_map = config_map
    else:
        if config != get_config_file():
            raise FileNotFoundError(errno.ENOENT, 'No such file', config)


@cli.command()
@click.argument('cluster-name')
@click.option('--num-slaves', type=int, required=True)
@click.option('--install-hdfs/--no-install-hdfs', default=True)
@click.option('--hdfs-version')
@click.option('--install-spark/--no-install-spark', default=True)
@click.option('--spark-version',
              help="Spark release version to install.")
@click.option('--spark-git-commit',
              help="Git commit hash to build Spark from. "
                   "--spark-version and --spark-git-commit are mutually exclusive.")
@click.option('--spark-git-repository',
              help="Git repository to clone Spark from.",
              default='https://github.com/apache/spark.git',
              show_default=True)
@click.option('--ec2-key-name')
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-instance-type', default='m3.medium', show_default=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-availability-zone')
@click.option('--ec2-ami')
@click.option('--ec2-user')
@click.option('--ec2-spot-price', type=float)
@click.option('--ec2-vpc-id')
@click.option('--ec2-subnet-id')
@click.option('--ec2-instance-profile-name')
@click.option('--ec2-placement-group')
@click.option('--ec2-tenancy', default='default')
@click.option('--ec2-ebs-optimized/--no-ec2-ebs-optimized', default=False)
@click.option('--ec2-instance-initiated-shutdown-behavior', default='stop',
              type=click.Choice(['stop', 'terminate']))
@click.pass_context
def launch(
        cli_context,
        cluster_name, num_slaves,
        install_hdfs,
        hdfs_version,
        install_spark,
        spark_version,
        spark_git_commit,
        spark_git_repository,
        ec2_key_name,
        ec2_identity_file,
        ec2_instance_type,
        ec2_region,
        ec2_availability_zone,
        ec2_ami,
        ec2_user,
        ec2_spot_price,
        ec2_vpc_id,
        ec2_subnet_id,
        ec2_instance_profile_name,
        ec2_placement_group,
        ec2_tenancy,
        ec2_ebs_optimized,
        ec2_instance_initiated_shutdown_behavior):
    """
    Launch a new cluster.
    """
    modules = []

    if install_hdfs:
        if not hdfs_version:
            # TODO: Custom exception for option dependencies.
            print(
                "Error: Cannot install HDFS. Missing option \"--hdfs-version\".",
                file=sys.stderr)
            sys.exit(2)
        hdfs = HDFS(version=hdfs_version)
        modules += [hdfs]
    if install_spark:
        if ((not spark_version and not spark_git_commit) or
                (spark_version and spark_git_commit)):
            # TODO: API for capturing option dependencies like this.
            print(
                'Error: Cannot install Spark. Exactly one of "--spark-version" or '
                '"--spark-git-commit" must be specified.',
                file=sys.stderr)
            print("--spark-version:", spark_version, file=sys.stderr)
            print("--spark-git-commit:", spark_git_commit, file=sys.stderr)
            sys.exit(2)
        else:
            if spark_version:
                spark = Spark(version=spark_version)
            elif spark_git_commit:
                print("Warning: Building Spark takes a long time. "
                      "e.g. 15-20 minutes on an m3.xlarge instance on EC2.")
                spark = Spark(git_commit=spark_git_commit,
                              git_repository=spark_git_repository)
            modules += [spark]

    if cli_context.obj['provider'] == 'ec2':
        return launch_ec2(
            cluster_name=cluster_name, num_slaves=num_slaves, modules=modules,
            key_name=ec2_key_name,
            identity_file=ec2_identity_file,
            instance_type=ec2_instance_type,
            region=ec2_region,
            availability_zone=ec2_availability_zone,
            ami=ec2_ami,
            user=ec2_user,
            spot_price=ec2_spot_price,
            vpc_id=ec2_vpc_id,
            subnet_id=ec2_subnet_id,
            instance_profile_name=ec2_instance_profile_name,
            placement_group=ec2_placement_group,
            tenancy=ec2_tenancy,
            ebs_optimized=ec2_ebs_optimized,
            instance_initiated_shutdown_behavior=ec2_instance_initiated_shutdown_behavior)
    else:
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


def get_or_create_ec2_security_groups(
        *,
        cluster_name,
        vpc_id,
        region) -> 'List[boto.ec2.securitygroup.SecurityGroup]':
    """
    If they do not already exist, create all the security groups needed for a
    Flintrock cluster.
    """
    connection = boto.ec2.connect_to_region(region_name=region)

    SecurityGroupRule = namedtuple(
        'SecurityGroupRule', [
            'ip_protocol',
            'from_port',
            'to_port',
            'src_group',
            'cidr_ip'])

    # TODO: Make these into methods, since we need this logic (though simple)
    #       in multiple places. (?)
    flintrock_group_name = 'flintrock'
    cluster_group_name = 'flintrock-' + cluster_name

    search_results = connection.get_all_security_groups(
        filters={
            'group-name': [flintrock_group_name, cluster_group_name]
        })

    # The Flintrock group is common to all Flintrock clusters and authorizes client traffic
    # to them.
    flintrock_group = next((sg for sg in search_results if sg.name == flintrock_group_name), None)

    # The cluster group is specific to one Flintrock cluster and authorizes intra-cluster
    # communication.
    cluster_group = next((sg for sg in search_results if sg.name == cluster_group_name), None)

    if not flintrock_group:
        flintrock_group = connection.create_security_group(
            name=flintrock_group_name,
            description="flintrock base group",
            vpc_id=vpc_id)

    # Rules for the client interacting with the cluster.
    flintrock_client_ip = (
        urllib.request.urlopen('http://checkip.amazonaws.com/')
        .read().decode('utf-8').strip())
    flintrock_client_cidr = '{ip}/32'.format(ip=flintrock_client_ip)

    client_rules = [
        # SSH
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=22,
            to_port=22,
            cidr_ip=flintrock_client_cidr,
            src_group=None),
        # HDFS
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=50070,
            to_port=50070,
            cidr_ip=flintrock_client_cidr,
            src_group=None),
        # Spark
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=8080,
            to_port=8081,
            cidr_ip=flintrock_client_cidr,
            src_group=None),
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=4040,
            to_port=4040,
            cidr_ip=flintrock_client_cidr,
            src_group=None)
    ]

    # TODO: Don't try adding rules that already exist.
    # TODO: Add rules in one shot.
    for rule in client_rules:
        try:
            flintrock_group.authorize(**rule._asdict())
        except boto.exception.EC2ResponseError as e:
            if e.error_code != 'InvalidPermission.Duplicate':
                print("Error adding rule: {r}".format(r=rule))
                raise

    # Rules for internal cluster communication.
    if not cluster_group:
        cluster_group = connection.create_security_group(
            name=cluster_group_name,
            description="Flintrock cluster group",
            vpc_id=vpc_id)

    cluster_rules = [
        SecurityGroupRule(
            ip_protocol='icmp',
            from_port=-1,
            to_port=-1,
            src_group=cluster_group,
            cidr_ip=None),
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=0,
            to_port=65535,
            src_group=cluster_group,
            cidr_ip=None),
        SecurityGroupRule(
            ip_protocol='udp',
            from_port=0,
            to_port=65535,
            src_group=cluster_group,
            cidr_ip=None)
    ]

    # TODO: Don't try adding rules that already exist.
    # TODO: Add rules in one shot.
    for rule in cluster_rules:
        try:
            cluster_group.authorize(**rule._asdict())
        except boto.exception.EC2ResponseError as e:
            if e.error_code != 'InvalidPermission.Duplicate':
                print("Error adding rule: {r}".format(r=rule))
                raise

    return [flintrock_group, cluster_group]


def get_ec2_block_device_map(
        *,
        ami: str,
        region: str) -> boto.ec2.blockdevicemapping.BlockDeviceMapping:
    """
    Get the block device map we should assign to instances launched from a given AMI.

    This is how we configure storage on the instance.
    """
    connection = boto.ec2.connect_to_region(region_name=region)

    image = connection.get_image(ami)
    root_device = boto.ec2.blockdevicemapping.BlockDeviceType(
        # Max root volume size for instance store-backed AMIs is 10 GiB.
        # See: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/add-instance-store-volumes.html
        size=30 if image.root_device_type == 'ebs' else 10,  # GiB
        volume_type='gp2',  # general-purpose SSD
        delete_on_termination=True)

    block_device_map = boto.ec2.blockdevicemapping.BlockDeviceMapping()
    block_device_map[image.root_device_name] = root_device

    for i in range(12):
        ephemeral_device = boto.ec2.blockdevicemapping.BlockDeviceType(
            ephemeral_name='ephemeral' + str(i))
        ephemeral_device_name = '/dev/sd' + string.ascii_lowercase[i + 1]
        block_device_map[ephemeral_device_name] = ephemeral_device

    return block_device_map


@timeit
def launch_ec2(
        *,
        cluster_name, num_slaves, modules,
        key_name, identity_file,
        instance_type,
        region,
        availability_zone,
        ami,
        user,
        spot_price=None,
        vpc_id, subnet_id,
        instance_profile_name,
        placement_group,
        tenancy="default", ebs_optimized=False,
        instance_initiated_shutdown_behavior="stop"):
    """
    Launch a fully functional cluster on EC2 with the specified configuration
    and installed modules.
    """
    try:
        get_cluster_instances_ec2(
            cluster_name=cluster_name,
            region=region)
    except ClusterNotFound as e:
        pass
    else:
        print("Cluster already exists: {c}".format(c=cluster_name), file=sys.stderr)
        sys.exit(1)

    security_groups = get_or_create_ec2_security_groups(
        cluster_name=cluster_name,
        vpc_id=vpc_id,
        region=region)
    block_device_map = get_ec2_block_device_map(
        ami=ami,
        region=region)

    connection = boto.ec2.connect_to_region(region_name=region)

    num_instances = num_slaves + 1
    spot_requests = []
    cluster_instances = []

    try:
        if spot_price:
            print("Requesting {c} spot instances at a max price of ${p}...".format(
                c=num_instances, p=spot_price))

            spot_requests = connection.request_spot_instances(
                price=spot_price,
                image_id=ami,
                count=num_instances,
                key_name=key_name,
                instance_type=instance_type,
                block_device_map=block_device_map,
                instance_profile_name=instance_profile_name,
                placement=availability_zone,
                security_group_ids=[sg.id for sg in security_groups],
                subnet_id=subnet_id,
                placement_group=placement_group,
                ebs_optimized=ebs_optimized)

            request_ids = [r.id for r in spot_requests]
            pending_request_ids = request_ids

            while pending_request_ids:
                print("{grant} of {req} instances granted. Waiting...".format(
                    grant=num_instances - len(pending_request_ids),
                    req=num_instances))
                time.sleep(30)
                spot_requests = connection.get_all_spot_instance_requests(request_ids=request_ids)
                pending_request_ids = [r.id for r in spot_requests if r.state != 'active']

            print("All {c} instances granted.".format(c=num_instances))

            cluster_instances = connection.get_only_instances(
                instance_ids=[r.instance_id for r in spot_requests])
        else:
            print("Launching {c} instances...".format(c=num_instances))

            reservation = connection.run_instances(
                image_id=ami,
                min_count=num_instances,
                max_count=num_instances,
                key_name=key_name,
                instance_type=instance_type,
                block_device_map=block_device_map,
                placement=availability_zone,
                security_group_ids=[sg.id for sg in security_groups],
                subnet_id=subnet_id,
                instance_profile_name=instance_profile_name,
                placement_group=placement_group,
                tenancy=tenancy,
                ebs_optimized=ebs_optimized,
                instance_initiated_shutdown_behavior=instance_initiated_shutdown_behavior)

            cluster_instances = reservation.instances

            time.sleep(10)  # AWS metadata eventual consistency tax.

        wait_for_cluster_state_ec2(
            cluster_instances=cluster_instances,
            state='running')

        master_instance = cluster_instances[0]
        slave_instances = cluster_instances[1:]

        connection.create_tags(
            resource_ids=[master_instance.id],
            tags={
                'flintrock-role': 'master',
                'Name': '{c}-master'.format(c=cluster_name)})
        connection.create_tags(
            resource_ids=[i.id for i in slave_instances],
            tags={
                'flintrock-role': 'slave',
                'Name': '{c}-slave'.format(c=cluster_name)})

        cluster_info = ClusterInfo(
            name=cluster_name,
            ssh_key_pair=generate_ssh_key_pair(),
            user=user,
            # Mystery: Why don't IP addresses work here?
            # master_host=master_instance.ip_address,
            # slave_hosts=[i.ip_address for i in slave_instances],
            master_host=master_instance.public_dns_name,
            slave_hosts=[i.public_dns_name for i in slave_instances],
            storage_dirs={
                'root': None,
                'ephemeral': None,
                'persistent': None
            })

        # TODO: Abstract away. No-one wants to see this async shite here.
        loop = asyncio.get_event_loop()

        tasks = []
        for instance in cluster_instances:
            # TODO: Use parameter names for run_in_executor() once Python 3.4.4 is released.
            #       Until then, we leave them out to maintain compatibility across Python 3.4
            #       and 3.5.
            # See: http://stackoverflow.com/q/32873974/
            task = loop.run_in_executor(
                None,
                functools.partial(
                    provision_node,
                    modules=modules,
                    user=user,
                    host=instance.ip_address,
                    identity_file=identity_file,
                    cluster_info=cluster_info))
            tasks.append(task)
        done, _ = loop.run_until_complete(asyncio.wait(tasks))

        # Is this the right way to make sure no coroutine failed?
        for future in done:
            future.result()

        loop.close()

        print("All {c} instances provisioned.".format(
            c=len(cluster_instances)))

        master_ssh_client = get_ssh_client(
            user=user,
            host=master_instance.ip_address,
            identity_file=identity_file)

        with master_ssh_client:
            # TODO: This manifest may need to be more full-featured to support
            #       adding nodes to a cluster.
            manifest = {
                'modules': [[type(m).__name__, m.version] for m in modules]}
            # The manifest tells us how the cluster is configured. We'll need this
            # when we resize the cluster or restart it.
            ssh_check_output(
                client=master_ssh_client,
                command="""
                    echo {m} > /home/{u}/.flintrock-manifest.json
                """.format(
                    m=shlex.quote(json.dumps(manifest, indent=4, sort_keys=True)),
                    u=shlex.quote(user)))

            for module in modules:
                module.configure_master(
                    ssh_client=master_ssh_client,
                    cluster_info=cluster_info)

        # NOTE: We sleep here so that the slave services have time to come up.
        #       If we refactor stuff to have a start_slave() that blocks until
        #       the slave is fully up, then we won't need this sleep anymore.
        if modules:
            time.sleep(30)

        for module in modules:
            module.health_check(master_host=cluster_info.master_host)

    except (Exception, KeyboardInterrupt) as e:
        print(e, file=sys.stderr)

        if spot_requests:
            # TODO: Do this only if there are pending requests.
            print("Canceling spot instance requests...", file=sys.stderr)
            request_ids = [r.id for r in spot_requests]
            connection.cancel_spot_instance_requests(
                request_ids=request_ids)
            # Make sure we have the latest information on any launched spot instances.
            spot_requests = connection.get_all_spot_instance_requests(
                request_ids=request_ids)
            instance_ids = [r.instance_id for r in spot_requests if r.instance_id]
            if instance_ids:
                cluster_instances = connection.get_only_instances(
                    instance_ids=instance_ids)

        if cluster_instances:
            yes = click.confirm(
                text="Do you want to terminate the {c} instances created by this operation?"
                     .format(c=len(cluster_instances)),
                err=True,
                default=True)

            if yes:
                print("Terminating instances...", file=sys.stderr)
                connection.terminate_instances(
                    instance_ids=[instance.id for instance in cluster_instances])

        sys.exit(1)
    # finally:
    #     print("Terminating all {c} instances...".format(
    #         c=len(cluster_instances)))
    #     connection.terminate_instances(
    #         instance_ids=[instance.id for instance in cluster_instances])


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


class ClusterNotFound(Exception):
    pass


# TODO: This function should probably return a ClusterInfo tuple with additional,
#       provider-specific fields. This can eventually morph into a proper class
#       with provider specific implementations.
def get_cluster_instances_ec2(
        *,
        cluster_name: str,
        region: str) -> (boto.ec2.instance.Instance, list):
    """
    Get the instances for an EC2 cluster.
    """
    connection = boto.ec2.connect_to_region(region_name=region)

    cluster_instances = connection.get_only_instances(
        filters={
            'instance.group-name': 'flintrock-' + cluster_name
        })

    if not cluster_instances:
        raise ClusterNotFound("No such cluster: {c}".format(c=cluster_name))

    # TODO: Raise errors if a cluster has no master or no slaves.
    master_instance = list(filter(
        lambda i: i.tags['flintrock-role'] == 'master',
        cluster_instances))[0]
    slave_instances = list(filter(
        lambda i: i.tags['flintrock-role'] != 'master',
        cluster_instances))

    return master_instance, slave_instances


@cli.command()
@click.argument('cluster-name')
# @click.confirmation_option(help="Are you sure you want to destroy this cluster?")
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.option('--ec2-region', default='us-east-1', show_default=True)
# TODO: Always delete cluster security group. People shouldn't be adding stuff to it.
#       Instead, provide option for cluster to be assigned to additional, pre-existing
#       security groups.
@click.pass_context
def destroy(cli_context, cluster_name, assume_yes, ec2_region):
    """
    Destroy a cluster.
    """
    if cli_context.obj['provider'] == 'ec2':
        destroy_ec2(
            cluster_name=cluster_name,
            assume_yes=assume_yes,
            region=ec2_region)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


# assume_yes defaults to True here for library use (as opposed to command-line use,
# where the default is configured via Click).
def destroy_ec2(*, cluster_name, assume_yes=True, region):
    try:
        master_instance, slave_instances = get_cluster_instances_ec2(
            cluster_name=cluster_name,
            region=region)
        cluster_instances = [master_instance] + slave_instances
    except ClusterNotFound as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    if not assume_yes:
        print_cluster_info_ec2(
            cluster_name=cluster_name,
            master_instance=master_instance,
            slave_instances=slave_instances)
        click.confirm(
            text="Are you sure you want to destroy this cluster?",
            abort=True)

    # TODO: Figure out if we want to use "node" instead of "instance" when
    #       communicating with the user, even if we're talking about doing things
    #       to EC2 instances. Spark docs definitely favor "node".
    print("Terminating {c} instances...".format(c=len(cluster_instances)))
    connection = boto.ec2.connect_to_region(region_name=region)
    connection.terminate_instances(
        instance_ids=[instance.id for instance in cluster_instances])

    # TODO: Destroy cluster security group. We're not reusing it.


def add_slaves(provider, cluster_name, num_slaves, provider_options):
    # Need concept of cluster state so we can add slaves with the same config.
    # Otherwise we must ask unreliable user to respecify slave config.
    pass


def add_slaves_ec2(cluster_name, num_slaves, identity_file):
    pass


def remove_slaves(provider, cluster_name, num_slaves, provider_options, assume_yes=False):
    pass


def remove_slaves_ec2(cluster_name, num_slaves, assume_yes=True):
    pass


def wait_for_cluster_state_ec2(*, cluster_instances: list, state: str):
    """
    Wait for all the instances in a cluster to reach a specific state.
    """
    while True:
        for instance in cluster_instances:
            if instance.state == state:
                continue
            else:
                time.sleep(3)
                instance.update()
                break
        else:
            break


def get_cluster_state_ec2(cluster_instances: list) -> str:
    """
    Get the state of an EC2 cluster.

    This is distinct from the state of Spark on the cluster. At some point the two
    concepts should be rationalized somehow.
    """
    instance_states = set(instance.state for instance in cluster_instances)

    if len(instance_states) == 1:
        return next(iter(instance_states))
    else:
        return 'inconsistent'


def print_cluster_info_ec2(
        *,
        cluster_name: str,
        master_instance: boto.ec2.instance.Instance,
        slave_instances: list):
    """
    Print information about an EC2 cluster to screen in a YAML-compatible format.

    This is the current solution until cluster methods are centralized under a
    FlintrockCluster class, or something similar.
    """
    cluster_state = get_cluster_state_ec2(
        cluster_instances=[master_instance] + slave_instances)

    # Mark boundaries of YAML output.
    # See: http://yaml.org/spec/current.html#id2525905
    # print('---')

    print(cluster_name + ':')
    print('  state: {s}'.format(s=cluster_state))
    print('  node-count: {nc}'.format(nc=1 + len(slave_instances)))
    if cluster_state == 'running':
        print('  master:', master_instance.public_dns_name)
        print('\n    - '.join(['  slaves:'] + [i.public_dns_name for i in slave_instances]))

    # print('...')


@cli.command()
@click.argument('cluster-name', required=False)
@click.option('--master-hostname-only', is_flag=True, default=False)
@click.option('--ec2-region')
@click.pass_context
def describe(
        cli_context,
        cluster_name,
        master_hostname_only,
        ec2_region):
    """
    Describe an existing cluster.

    Leave out the cluster name to find all Flintrock-managed clusters.
    """
    if cli_context.obj['provider'] == 'ec2':
        describe_ec2(
            cluster_name=cluster_name,
            master_hostname_only=master_hostname_only,
            region=ec2_region)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


def describe_ec2(*, cluster_name, master_hostname_only=False, region):
    if cluster_name:
        try:
            master_instance, slave_instances = get_cluster_instances_ec2(
                cluster_name=cluster_name,
                region=region)
            cluster_instances = [master_instance] + slave_instances
        except ClusterNotFound as e:
            print(e, file=sys.stderr)
            sys.exit(1)

        if master_hostname_only:
            print(master_instance.public_dns_name)
        else:
            print_cluster_info_ec2(
                cluster_name=cluster_name,
                master_instance=master_instance,
                slave_instances=slave_instances)
    else:
        connection = boto.ec2.connect_to_region(region_name=region)

        all_clusters_instances = connection.get_only_instances(
            filters={
                'instance.group-name': 'flintrock-' + cluster_name if cluster_name else 'flintrock'
            })
        security_groups = itertools.chain.from_iterable(
            [i.groups for i in all_clusters_instances])
        security_group_names = {g.name for g in security_groups if g.name.startswith('flintrock-')}
        cluster_names = [n.replace('flintrock-', '', 1) for n in security_group_names]

        clusters = {}
        for cluster_name in cluster_names:
            master_instance = None
            slave_instances = []

            for instance in all_clusters_instances:
                if ('flintrock-' + cluster_name) in {g.name for g in instance.groups}:
                    if instance.tags['flintrock-role'] == 'master':
                        master_instance = instance
                    elif instance.tags['flintrock-role'] != 'master':
                        slave_instances.append(instance)

            clusters[cluster_name] = {
                'master_instance': master_instance,
                'slave_instances': slave_instances}

        if master_hostname_only:
            for cluster_name in sorted(cluster_names):
                print(cluster_name + ':', clusters[cluster_name]['master_instance'].public_dns_name)
        else:
            print("{n} cluster{s} found.".format(
                n=len(cluster_names),
                s='' if len(cluster_names) == 1 else 's'))
            if cluster_names:
                print('---')
                for cluster_name in sorted(cluster_names):
                    print_cluster_info_ec2(
                        cluster_name=cluster_name,
                        master_instance=clusters[cluster_name]['master_instance'],
                        slave_instances=clusters[cluster_name]['slave_instances'])


def ssh(*, user: str, host: str, identity_file: str):
    """
    SSH into a host for interactive use.
    """
    ret = subprocess.call([
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-i', identity_file,
        '{u}@{h}'.format(u=user, h=host)])


# TODO: Provide different command or option for going straight to Spark Shell.
@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
# TODO: Move identity-file to global, non-provider-specific option. (?)
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-user')
@click.pass_context
def login(cli_context, cluster_name, ec2_region, ec2_identity_file, ec2_user):
    """
    Login to the master of an existing cluster.
    """
    if cli_context.obj['provider'] == 'ec2':
        login_ec2(
            cluster_name=cluster_name,
            region=ec2_region,
            identity_file=ec2_identity_file,
            user=ec2_user)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


def login_ec2(cluster_name, region, identity_file, user):
    try:
        master_instance, slave_instances = get_cluster_instances_ec2(
            cluster_name=cluster_name,
            region=region)
        cluster_instances = [master_instance] + slave_instances
    except ClusterNotFound as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    ssh(
        user=user,
        host=master_instance.public_dns_name,
        identity_file=identity_file)


@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
# TODO: Move identity-file to global, non-provider-specific option. (?)
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-user')
@click.pass_context
def start(cli_context, cluster_name, ec2_region, ec2_identity_file, ec2_user):
    """
    Start an existing, stopped cluster.
    """
    if cli_context.obj['provider'] == 'ec2':
        start_ec2(
            cluster_name=cluster_name,
            region=ec2_region,
            identity_file=ec2_identity_file,
            user=ec2_user)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


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


@timeit
def start_ec2(*, cluster_name: str, region: str, identity_file: str, user: str):
    """
    Start an existing, stopped cluster on EC2.
    """
    try:
        master_instance, slave_instances = get_cluster_instances_ec2(
            cluster_name=cluster_name,
            region=region)
        cluster_instances = [master_instance] + slave_instances
    except ClusterNotFound as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    cluster_state = get_cluster_state_ec2(cluster_instances)
    if cluster_state != 'stopped':
        print("Cannot start cluster in state:", cluster_state, file=sys.stderr)
        sys.exit(1)

    print("Starting {c} instances...".format(c=len(cluster_instances)))
    connection = boto.ec2.connect_to_region(region_name=region)
    connection.start_instances(
        instance_ids=[instance.id for instance in cluster_instances])

    wait_for_cluster_state_ec2(
        cluster_instances=cluster_instances,
        state='running')

    master_ssh_client = get_ssh_client(
        user=user,
        host=master_instance.ip_address,
        identity_file=identity_file)

    with master_ssh_client:
        manifest_raw = ssh_check_output(
            client=master_ssh_client,
            command="""
                cat /home/{u}/.flintrock-manifest.json
            """.format(u=shlex.quote(user)))
        # TODO: Reconsider where this belongs. In the manifest? We can implement
        #       ephemeral storage support as a Flintrock module, and add methods to
        #       serialize and deserialize critical module info like installed versions
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
    storage_dirs = {
        'root': '/media/root',
        'ephemeral': sorted(ephemeral_dirs_raw.splitlines()),
        'persistent': None
    }

    cluster_info = ClusterInfo(
        name=cluster_name,
        ssh_key_pair=None,
        user=user,
        # Mystery: Why don't IP addresses work here?
        # master_host=master_instance.ip_address,
        # slave_hosts=[i.ip_address for i in slave_instances],
        master_host=master_instance.public_dns_name,
        slave_hosts=[i.public_dns_name for i in slave_instances],
        storage_dirs=storage_dirs)

    modules = []
    for [module_name, version] in manifest['modules']:
        module = globals()[module_name](version)
        modules.append(module)

    loop = asyncio.get_event_loop()

    tasks = []
    for instance in cluster_instances:
        # TODO: Use parameter names for run_in_executor() once Python 3.4.4 is released.
        #       Until then, we leave them out to maintain compatibility across Python 3.4
        #       and 3.5.
        # See: http://stackoverflow.com/q/32873974/
        task = loop.run_in_executor(
            None,
            functools.partial(
                start_node,
                modules=modules,
                user=user,
                host=instance.ip_address,
                identity_file=identity_file,
                cluster_info=cluster_info))
        tasks.append(task)
    done, _ = loop.run_until_complete(asyncio.wait(tasks))

    # Is this is the right way to make sure no coroutine failed?
    for future in done:
        future.result()

    loop.close()

    master_ssh_client = get_ssh_client(
        user=user,
        host=master_instance.ip_address,
        identity_file=identity_file)

    with master_ssh_client:
        for module in modules:
            module.configure_master(
                ssh_client=master_ssh_client,
                cluster_info=cluster_info)

    # NOTE: We sleep here so that the slave services have time to come up.
    #       If we refactor stuff to have a start_slave() that blocks until
    #       the slave is fully up, then we won't need this sleep anymore.
    if modules:
        time.sleep(30)

    for module in modules:
        module.health_check(master_host=master_instance.ip_address)


@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.pass_context
def stop(cli_context, cluster_name, ec2_region, assume_yes):
    """
    Stop an existing, running cluster.
    """
    if cli_context.obj['provider'] == 'ec2':
        stop_ec2(cluster_name=cluster_name, region=ec2_region, assume_yes=assume_yes)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


@timeit
def stop_ec2(cluster_name, region, assume_yes=True):
    try:
        master_instance, slave_instances = get_cluster_instances_ec2(
            cluster_name=cluster_name,
            region=region)
        cluster_instances = [master_instance] + slave_instances
    except ClusterNotFound as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    cluster_state = get_cluster_state_ec2(cluster_instances)
    if cluster_state == 'stopped':
        print("Cluster is already stopped.")
        sys.exit(0)

    if not assume_yes:
        print_cluster_info_ec2(
            cluster_name=cluster_name,
            master_instance=master_instance,
            slave_instances=slave_instances)
        click.confirm(
            text="Are you sure you want to stop this cluster?",
            abort=True)

    print("Stopping {c} instances...".format(c=len(cluster_instances)))
    connection = boto.ec2.connect_to_region(region_name=region)
    connection.stop_instances(
        instance_ids=[instance.id for instance in cluster_instances])

    wait_for_cluster_state_ec2(
        cluster_instances=cluster_instances,
        state='stopped')
    print("{c} is now stopped.".format(c=cluster_name))


@cli.command(name='run-command')
@click.argument('cluster-name')
@click.argument('command', nargs=-1)
@click.option('--master-only', help="Run on the master only.", is_flag=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-user')
@click.pass_context
def run_command(
        cli_context,
        cluster_name,
        command,
        master_only,
        ec2_region,
        ec2_identity_file,
        ec2_user):
    """
    Run a shell command on a cluster.

    Examples:

        flintrock run-command my-cluster 'touch /tmp/flintrock'
        flintrock run-command my-cluster -- yum install -y package

    Flintrock will return a non-zero code if any of the cluster nodes raises an error
    while running the command.
    """
    if cli_context.obj['provider'] == 'ec2':
        run_command_ec2(
            cluster_name=cluster_name,
            command=command,
            master_only=master_only,
            region=ec2_region,
            identity_file=ec2_identity_file,
            user=ec2_user)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


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


@timeit
def run_command_ec2(cluster_name, command, master_only, region, identity_file, user):
    try:
        master_instance, slave_instances = get_cluster_instances_ec2(
            cluster_name=cluster_name,
            region=region)
        cluster_instances = [master_instance] + slave_instances
    except ClusterNotFound as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    if get_cluster_state_ec2(cluster_instances) != 'running':
        print("Cluster is not in a running state.", file=sys.stderr)
        sys.exit(1)

    loop = asyncio.get_event_loop()

    if master_only:
        target_instances = [master_instance]
    else:
        target_instances = cluster_instances

    print("Running command on {c} instance{s}...".format(
        c=len(target_instances),
        s='' if len(target_instances) == 1 else 's'))

    tasks = []
    for instance in target_instances:
        # TODO: Use parameter names for run_in_executor() once Python 3.4.4 is released.
        #       Until then, we leave them out to maintain compatibility across Python 3.4
        #       and 3.5.
        # See: http://stackoverflow.com/q/32873974/
        task = loop.run_in_executor(
            None,
            functools.partial(
                run_command_node,
                user=user,
                host=instance.ip_address,
                identity_file=identity_file,
                command=command))
        tasks.append(task)

    try:
        loop.run_until_complete(asyncio.gather(*tasks))
    # TODO: Cancel cleanly from hung commands. The below doesn't work.
    #       Probably related to: http://stackoverflow.com/q/29177490/
    # except KeyboardInterrupt as e:
    #     sys.exit(1)
    except Exception as e:
        print("At least one node raised an error:", e, file=sys.stderr)
        sys.exit(1)

    loop.close()


@cli.command(name='copy-file')
@click.argument('cluster-name')
@click.argument('local_path', type=click.Path(exists=True, dir_okay=False))
@click.argument('remote_path', type=click.Path())
@click.option('--master-only', help="Copy to the master only.", is_flag=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-user')
@click.option('--assume-yes/--no-assume-yes', default=False, help="Prompt before large uploads.")
@click.pass_context
def copy_file(
        cli_context,
        cluster_name,
        local_path,
        remote_path,
        master_only,
        ec2_region,
        ec2_identity_file,
        ec2_user,
        assume_yes):
    """
    Copy a local file up to a cluster.

    This will copy the file to the same path on each node of the cluster.

    Examples:

        flintrock copy-file my-cluster /tmp/file.102.txt /tmp/file.txt
        flintrock copy-file my-cluster /tmp/spark-defaults.conf /tmp/

    Flintrock will return a non-zero code if any of the cluster nodes raises an error.
    """
    # We assume POSIX for the remote path since Flintrock
    # only supports clusters running CentOS / Amazon Linux.
    if not posixpath.basename(remote_path):
        remote_path = posixpath.join(remote_path, os.path.basename(local_path))

    if cli_context.obj['provider'] == 'ec2':
        copy_file_ec2(
            cluster_name=cluster_name,
            local_path=local_path,
            remote_path=remote_path,
            master_only=master_only,
            region=ec2_region,
            identity_file=ec2_identity_file,
            user=ec2_user,
            assume_yes=assume_yes)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


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


@timeit
def copy_file_ec2(*, cluster_name, local_path, remote_path, master_only=False, region, identity_file, user, assume_yes=True):
    try:
        master_instance, slave_instances = get_cluster_instances_ec2(
            cluster_name=cluster_name,
            region=region)
        cluster_instances = [master_instance] + slave_instances
    except ClusterNotFound as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    if get_cluster_state_ec2(cluster_instances) != 'running':
        print("Cluster is not in a running state.", file=sys.stderr)
        sys.exit(1)

    if not assume_yes and not master_only:
        file_size_bytes = os.path.getsize(local_path)
        num_nodes = len(cluster_instances)
        total_size_bytes = file_size_bytes * num_nodes

        if total_size_bytes > 10 ** 6:
            print("WARNING:")
            print(
                format_message(
                    message="""\
                        You are trying to upload {total_size} bytes ({size} bytes x {count}
                        nodes in {cluster}). Depending on your upload bandwidth, this may take
                        a long time.
                        You may be better off uploading this file to a storage service like
                        Amazon S3 and downloading it from there to the cluster using
                        `flintrock run-command ...`.
                        """.format(
                            size=file_size_bytes,
                            count=num_nodes,
                            cluster=cluster_name,
                            total_size=total_size_bytes),
                    wrap=60))
            click.confirm(
                text="Are you sure you want to continue?",
                default=True,
                abort=True)

    loop = asyncio.get_event_loop()

    if master_only:
        target_instances = [master_instance]
    else:
        target_instances = cluster_instances

    print("Copying file to {c} instance{s}...".format(
        c=len(target_instances),
        s='' if len(target_instances) == 1 else 's'))

    tasks = []
    for instance in target_instances:
        # TODO: Use parameter names for run_in_executor() once Python 3.4.4 is released.
        #       Until then, we leave them out to maintain compatibility across Python 3.4
        #       and 3.5.
        # See: http://stackoverflow.com/q/32873974/
        task = loop.run_in_executor(
            None,
            functools.partial(
                copy_file_node,
                user=user,
                host=instance.ip_address,
                identity_file=identity_file,
                local_path=local_path,
                remote_path=remote_path))
        tasks.append(task)

    try:
        loop.run_until_complete(asyncio.gather(*tasks))
    # TODO: Cancel cleanly from hung commands. The below doesn't work.
    #       Probably related to: http://stackoverflow.com/q/29177490/
    # except KeyboardInterrupt as e:
    #     sys.exit(1)
    except Exception as e:
        print("At least one node raised an error:", e, file=sys.stderr)
        sys.exit(1)

    loop.close()


def format_message(*, message: str, indent: int=4, wrap: int=70):
    """
    Format a lengthy message for printing to screen.
    """
    return textwrap.indent(
        textwrap.fill(
            textwrap.dedent(text=message),
            width=wrap),
        prefix=' ' * indent)


def normalize_keys(obj):
    """
    Used to map keys from config files to Python parameter names.
    """
    if type(obj) != dict:
        return obj
    else:
        return {k.replace('-', '_'): normalize_keys(v) for k, v in obj.items()}


def config_to_click(config: dict) -> dict:
    """
    Convert a dictionary of configurations loaded from a Flintrock config file
    to a dictionary that Click can use to set default options.
    """
    module_configs = {}

    if 'modules' in config:
        for module in config['modules']:
            if config['modules'][module]:
                module_configs.update(
                    {module + '_' + k: v for (k, v) in config['modules'][module].items()})

    ec2_configs = {
        'ec2_' + k: v for (k, v) in config['providers']['ec2'].items()}

    click = {
        'launch': dict(
            list(config['launch'].items()) +
            list(ec2_configs.items()) +
            list(module_configs.items())),
        'describe': ec2_configs,
        'destroy': ec2_configs,
        'login': ec2_configs,
        'start': ec2_configs,
        'stop': ec2_configs,
        'run-command': ec2_configs,
        'copy-file': ec2_configs,
    }

    # TODO: Use a different name. click is a module.
    return click


@cli.command()
@click.option('--locate', is_flag=True, default=False,
              help="Don't open an editor. "
              "Just open the folder containing the configuration file.")
@click.pass_context
def configure(cli_context, locate):
    """
    Configure Flintrock's defaults.

    This will open Flintrock's configuration file in your default YAML editor so
    you can set your defaults.
    """
    config_file = get_config_file()

    if not os.path.isfile(config_file):
        print("Initializing config file from template...")
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        shutil.copyfile(
            src=os.path.join(THIS_DIR, 'config.yaml.template'),
            dst=config_file)
        os.chmod(config_file, mode=0o644)

    click.launch(config_file, locate=locate)


def main():
    # We pass in obj so we can add attributes to it, like provider, which
    # get shared by all commands.
    # See: http://click.pocoo.org/6/api/#click.Context
    cli(obj={})
