import os
import posixpath
import errno
import json
import resource
import sys
import shutil
import textwrap
import urllib.parse
import urllib.request
import warnings
import logging

# External modules
import click
import yaml
# We import botocore here so we can catch when the user tries to
# access AWS without having their credentials configured and provide
# a friendly error message. Apart from that, flintrock.py should
# not really know anything about EC2 or boto since that is delegated
# to ec2.py.
import botocore

# Flintrock modules
from . import ec2
from .exceptions import (
    UsageError,
    UnsupportedProviderError,
    NothingToDo,
    Error)
from flintrock import __version__
from .util import spark_hadoop_build_version
from .services import HDFS, Spark  # TODO: Remove this dependency.

FROZEN = getattr(sys, 'frozen', False)

if FROZEN:
    THIS_DIR = sys._MEIPASS
else:
    THIS_DIR = os.path.dirname(os.path.realpath(__file__))


logger = logging.getLogger('flintrock.flintrock')


def format_message(*, message: str, indent: int=4, wrap: int=70):
    """
    Format a lengthy message for printing to screen.
    """
    return textwrap.indent(
        textwrap.fill(
            textwrap.dedent(text=message),
            width=wrap),
        prefix=' ' * indent)


def option_name_to_variable_name(option: str):
    """
    Convert an option name like `--ec2-user` to the Python name it gets mapped to,
    like `ec2_user`.
    """
    return option.replace('--', '', 1).replace('-', '_')


def variable_name_to_option_name(variable: str):
    """
    Convert a variable name like `ec2_user` to the Click option name it gets mapped to,
    like `--ec2-user`.
    """
    return '--' + variable.replace('_', '-')


def option_requires(
        *,
        option: str,
        conditional_value=None,
        requires_all: list=[],
        requires_any: list=[],
        scope: dict):
    """
    Raise an exception if an option's requirements are not met.

    The option's requirements are checked only if the option has a "truthy" value
    (i.e. it's not a "falsy" value like '', None, or False), and if its value is
    equal to conditional_value, if conditional_value is not None.

    requires_all: Every option in this list must be defined.
    requires_any: At least one option in this list must be defined.

    This function looks for values by converting the option names to their
    corresponding variable names (e.g. --option-a becomes option_a) and looking them
    up in the provided scope.
    """
    option_value = scope[option_name_to_variable_name(option)]

    if option_value and \
            (conditional_value is None or option_value == conditional_value):
        if requires_all:
            for required_option in requires_all:
                required_name = option_name_to_variable_name(required_option)
                if required_name not in scope or not scope[required_name]:
                    raise UsageError(
                        "Error: Missing option \"{missing_option}\" is required by "
                        "\"{option}{space}{conditional_value}\"."
                        .format(
                            missing_option=required_option,
                            option=option,
                            space=' ' if conditional_value is not None else '',
                            conditional_value=conditional_value if conditional_value is not None else ''))
        if requires_any:
            for required_option in requires_any:
                required_name = option_name_to_variable_name(required_option)
                if required_name in scope and scope[required_name] is not None:
                    break
            else:
                raise UsageError(
                    "Error: \"{option}{space}{conditional_value}\" requires at least "
                    "one of the following options to be set: {at_least}"
                    .format(
                        option=option,
                        space=' ' if conditional_value is not None else '',
                        conditional_value=conditional_value if conditional_value is not None else '',
                        at_least=', '.join(['"' + ra + '"' for ra in requires_any])))


def mutually_exclusive(*, options: list, scope: dict):
    """
    Raise an exception if more than one of the provided options is specified.

    This function looks for values by converting the option names to their
    corresponding variable names (e.g. --option-a becomes option_a) and looking them
    up in the provided scope.
    """
    mutually_exclusive_names = [option_name_to_variable_name(o) for o in options]

    used_options = set()
    for name, value in scope.items():
        if name in mutually_exclusive_names and scope[name]:  # is not None:
            used_options.add(name)

    if len(used_options) > 1:
        bad_option1 = used_options.pop()
        bad_option2 = used_options.pop()
        raise UsageError(
            "Error: \"{option1}\" and \"{option2}\" are mutually exclusive.\n"
            "  {option1}: {value1}\n"
            "  {option2}: {value2}"
            .format(
                option1=variable_name_to_option_name(bad_option1),
                value1=scope[bad_option1],
                option2=variable_name_to_option_name(bad_option2),
                value2=scope[bad_option2]))


def get_config_file() -> str:
    """
    Get the path to Flintrock's default configuration file.
    """
    config_dir = click.get_app_dir(app_name='Flintrock')
    config_file = os.path.join(config_dir, 'config.yaml')
    return config_file


def configure_log(debug: bool):
    root_logger = logging.getLogger('flintrock')
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    if debug:
        root_logger.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(asctime)s - flintrock.%(module)-9s - %(levelname)-5s - %(message)s'))
    else:
        root_logger.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(message)s'))
    root_logger.addHandler(handler)


def build_hdfs_download_url(ctx, param, value):
    hdfs_version = ctx.params['hdfs_version']
    if value.endswith('.gz') or value.endswith('.tgz'):
        logger.warning(
            "Hadoop download source appears to point to a file, not a directory. "
            "Flintrock will not try to determine the correct file to download based on "
            "the Hadoop version."
        )
        hdfs_download_url = value
    else:
        hdfs_download_url = (value.rstrip('/') + '/hadoop-{v}.tar.gz')
    return hdfs_download_url.format(v=hdfs_version)


def build_spark_download_url(ctx, param, value):
    spark_version = ctx.params['spark_version']
    hadoop_version = ctx.params['hdfs_version']
    hadoop_build_version = spark_hadoop_build_version(hadoop_version)
    if value.endswith('.gz') or value.endswith('.tgz'):
        logger.warning(
            "Spark download source appears to point to a file, not a directory. "
            "Flintrock will not try to determine the correct file to download based on "
            "the Spark and Hadoop versions."
        )
        spark_download_url = value
    else:
        spark_download_url = (value.rstrip('/') + '/spark-{v}-bin-{hv}.tgz')
    return spark_download_url.format(
        v=spark_version,
        hv=hadoop_build_version,
    )


def validate_download_source(url):
    if 'spark' in url:
        software = 'Spark'
    elif 'hadoop' in url:
        software = 'Hadoop'
    else:
        software = 'software'

    parsed_url = urllib.parse.urlparse(url)

    if parsed_url.netloc == 'www.apache.org' and parsed_url.path == '/dyn/closer.lua':
        logger.warning(
            "Warning: "
            "Downloading {software} from an Apache mirror. Apache mirrors are "
            "often slow and unreliable, and typically only serve the most recent releases. "
            "We strongly recommend you specify a custom download source. "
            "For more background on this issue, please see: https://github.com/nchammas/flintrock/issues/238"
            .format(
                software=software,
            )
        )
        try:
            urllib.request.urlopen(url)
        except urllib.error.HTTPError as e:
            raise Error(
                "Error: Could not access {software} download. Maybe try a more recent release?\n"
                "  - Automatically redirected to: {url}\n"
                "  - HTTP error: {code}"
                .format(
                    software=software,
                    url=e.url,
                    code=e.code,
                )
            )


@click.group()
@click.option(
    '--config',
    help="Path to a Flintrock configuration file.",
    default=get_config_file())
@click.option('--provider', default='ec2', type=click.Choice(['ec2']))
@click.version_option(version=__version__)
# TODO: implement some solution like in https://github.com/pallets/click/issues/108
@click.option('--debug/--no-debug', default=False, help="Show debug information.")
@click.pass_context
def cli(cli_context, config, provider, debug):
    """
    Flintrock

    A command-line tool for launching Apache Spark clusters.
    """
    cli_context.obj['provider'] = provider

    if os.path.isfile(config):
        with open(config) as f:
            config_raw = yaml.safe_load(f)
            debug = config_raw.get('debug') or debug
            config_map = config_to_click(normalize_keys(config_raw))

        cli_context.default_map = config_map
    else:
        if config != get_config_file():
            raise FileNotFoundError(errno.ENOENT, 'No such file', config)
    configure_log(debug=debug)


@cli.command()
@click.argument('cluster-name')
@click.option('--num-slaves', type=click.IntRange(min=1), required=True)
@click.option('--java-version', type=click.IntRange(min=8), default=11)
@click.option('--install-hdfs/--no-install-hdfs', default=False)
@click.option('--hdfs-version', default='3.3.0')
@click.option('--hdfs-download-source',
              help=(
                  "URL to download Hadoop from. If an S3 URL, Flintrock will use the "
                  "AWS CLI from the cluster nodes to download it. "
                  "Flintrock will append the appropriate file name to the end "
                  "of the URL based on the Apache release file names here: "
                  "https://dist.apache.org/repos/dist/release/hadoop/common/"
              ),
              default='https://www.apache.org/dyn/closer.lua?action=download&filename=hadoop/common/hadoop-{v}/',
              show_default=True,
              callback=build_hdfs_download_url)
@click.option('--install-spark/--no-install-spark', default=True)
@click.option('--spark-executor-instances', default=1,
              help="How many executor instances per worker.")
@click.option('--spark-version',
              # Don't set a default here because it will conflict with
              # the config file if the git commit is set.
              # See: https://github.com/nchammas/flintrock/issues/190
              # default=,
              help="Spark release version to install.")
@click.option('--spark-download-source',
              help=(
                  "URL to download Spark from. If an S3 URL, Flintrock will use the "
                  "AWS CLI from the cluster nodes to download it. "
                  "Flintrock will append the appropriate file "
                  "name to the end of the URL based on the selected Hadoop version and "
                  "Apache release file names here: "
                  "https://dist.apache.org/repos/dist/release/spark/"
              ),
              default='https://www.apache.org/dyn/closer.lua?action=download&filename=spark/spark-{v}/',
              show_default=True,
              callback=build_spark_download_url)
@click.option('--spark-git-commit',
              help="Git commit to build Spark from. "
                   "Set to 'latest' to build Spark from the latest commit on the "
                   "repository's default branch.")
@click.option('--spark-git-repository',
              help="Git repository to clone Spark from.",
              default='https://github.com/apache/spark',
              show_default=True)
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.option('--ec2-key-name')
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-instance-type', default='m5.medium', show_default=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
# We set some of these defaults to empty strings because of boto3's parameter validation.
# See: https://github.com/boto/boto3/issues/400
@click.option('--ec2-availability-zone', default='')
@click.option('--ec2-ami')
@click.option('--ec2-user')
@click.option('--ec2-security-group', 'ec2_security_groups',
              multiple=True,
              help="Additional security groups names to assign to the instances. "
                   "You can specify this option multiple times.")
@click.option('--ec2-spot-price', type=float)
@click.option('--ec2-spot-request-duration', default='7d',
              help="Duration a spot request is valid (e.g. 3d 2h 1m).")
@click.option('--ec2-min-root-ebs-size-gb', type=int, default=30)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
@click.option('--ec2-subnet-id', default='')
@click.option('--ec2-instance-profile-name', default='')
@click.option('--ec2-placement-group', default='')
@click.option('--ec2-tenancy', default='default')
@click.option('--ec2-ebs-optimized/--no-ec2-ebs-optimized', default=False)
@click.option('--ec2-instance-initiated-shutdown-behavior', default='stop',
              type=click.Choice(['stop', 'terminate']))
@click.option('--ec2-user-data',
              type=click.File(mode='r', encoding='utf-8'),
              help="Path to EC2 user data script that will run on instance launch.")
@click.option('--ec2-tag', 'ec2_tags',
              callback=ec2.cli_validate_tags,
              multiple=True,
              help="Additional tags (e.g. 'Key,Value') to assign to the instances. "
                   "You can specify this option multiple times.")
@click.option('--ec2-authorize-access-from',
              callback=ec2.cli_validate_ec2_authorize_access,
              multiple=True,
              help=(
                  "Authorize cluster access from a specific source (e.g. on a private "
                  "network). The source can be a) a plain IP address, b) an IP "
                  "address in CIDR notation, or c) an EC2 Security Group ID. "
                  "Using this option disables automatic detection of client's public IP "
                  "address."
              ))
@click.pass_context
def launch(
        cli_context,
        cluster_name,
        num_slaves,
        java_version,
        install_hdfs,
        hdfs_version,
        hdfs_download_source,
        install_spark,
        spark_executor_instances,
        spark_version,
        spark_git_commit,
        spark_git_repository,
        spark_download_source,
        assume_yes,
        ec2_key_name,
        ec2_identity_file,
        ec2_instance_type,
        ec2_region,
        ec2_availability_zone,
        ec2_ami,
        ec2_user,
        ec2_security_groups,
        ec2_spot_price,
        ec2_spot_request_duration,
        ec2_min_root_ebs_size_gb,
        ec2_vpc_id,
        ec2_subnet_id,
        ec2_instance_profile_name,
        ec2_placement_group,
        ec2_tenancy,
        ec2_ebs_optimized,
        ec2_instance_initiated_shutdown_behavior,
        ec2_user_data,
        ec2_tags,
        ec2_authorize_access_from):
    """
    Launch a new cluster.
    """
    provider = cli_context.obj['provider']
    services = []

    option_requires(
        option='--install-hdfs',
        requires_all=['--hdfs-version'],
        scope=locals())
    option_requires(
        option='--install-spark',
        requires_any=[
            '--spark-version',
            '--spark-git-commit'],
        scope=locals())
    mutually_exclusive(
        options=[
            '--spark-version',
            '--spark-git-commit'],
        scope=locals())
    option_requires(
        option='--install-spark',
        requires_all=[
            '--hdfs-version'],
        scope=locals())
    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=[
            '--ec2-key-name',
            '--ec2-identity-file',
            '--ec2-instance-type',
            '--ec2-region',
            '--ec2-ami',
            '--ec2-user'],
        scope=locals())
    # The subnet is required for non-default VPCs because EC2 does not
    # support user-defined default subnets.
    # See: https://forums.aws.amazon.com/thread.jspa?messageID=707417
    #      https://github.com/mitchellh/packer/issues/1935#issuecomment-111235752
    option_requires(
        option='--ec2-vpc-id',
        requires_all=['--ec2-subnet-id'],
        scope=locals())

    check_external_dependency('ssh-keygen')

    if install_hdfs:
        validate_download_source(hdfs_download_source)
        hdfs = HDFS(
            version=hdfs_version,
            download_source=hdfs_download_source,
        )
        services += [hdfs]
    if install_spark:
        if spark_version:
            validate_download_source(spark_download_source)
            spark = Spark(
                spark_executor_instances=spark_executor_instances,
                version=spark_version,
                hadoop_version=hdfs_version,
                download_source=spark_download_source,
            )
        elif spark_git_commit:
            logger.warning(
                "Warning: Building Spark takes a long time. "
                "e.g. 15-20 minutes on an m5.xlarge instance on EC2.")
            if spark_git_commit == 'latest':
                spark_git_commit = get_latest_commit(spark_git_repository)
                logger.info("Building Spark at latest commit: {c}".format(c=spark_git_commit))
            spark = Spark(
                spark_executor_instances=spark_executor_instances,
                git_commit=spark_git_commit,
                git_repository=spark_git_repository,
                hadoop_version=hdfs_version,
            )
        services += [spark]

    if provider == 'ec2':
        cluster = ec2.launch(
            cluster_name=cluster_name,
            num_slaves=num_slaves,
            java_version=java_version,
            services=services,
            assume_yes=assume_yes,
            key_name=ec2_key_name,
            identity_file=ec2_identity_file,
            instance_type=ec2_instance_type,
            region=ec2_region,
            availability_zone=ec2_availability_zone,
            ami=ec2_ami,
            user=ec2_user,
            security_groups=ec2_security_groups,
            spot_price=ec2_spot_price,
            spot_request_duration=ec2_spot_request_duration,
            min_root_ebs_size_gb=ec2_min_root_ebs_size_gb,
            vpc_id=ec2_vpc_id,
            subnet_id=ec2_subnet_id,
            instance_profile_name=ec2_instance_profile_name,
            placement_group=ec2_placement_group,
            tenancy=ec2_tenancy,
            ebs_optimized=ec2_ebs_optimized,
            instance_initiated_shutdown_behavior=ec2_instance_initiated_shutdown_behavior,
            user_data=ec2_user_data,
            tags=ec2_tags,
            ec2_authorize_access_from=ec2_authorize_access_from)
    else:
        raise UnsupportedProviderError(provider)

    print("Cluster master: {}".format(cluster.master_host))
    print("Login with: flintrock login {}".format(cluster.name))


def get_latest_commit(github_repository: str):
    """
    Get the latest commit on the default branch of a repository hosted on GitHub.
    """
    parsed_url = urllib.parse.urlparse(github_repository)
    repo_domain, repo_path = parsed_url.netloc, parsed_url.path.strip('/')

    if repo_domain != 'github.com':
        raise UsageError(
            "Error: Getting the latest commit is only supported "
            "for repositories hosted on GitHub. "
            "Provided repository domain was: {d}".format(d=repo_domain))

    url = "https://api.github.com/repos/{rp}/commits".format(rp=repo_path)
    try:
        with urllib.request.urlopen(url) as response:
            result = json.loads(response.read().decode('utf-8'))
            return result[0]['sha']
    except Exception as e:
        raise Exception(
            "Could not get latest commit for repository: {r}"
            .format(r=repo_path)) from e


@cli.command()
@click.argument('cluster-name')
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
@click.pass_context
def destroy(cli_context, cluster_name, assume_yes, ec2_region, ec2_vpc_id):
    """
    Destroy a cluster.
    """
    provider = cli_context.obj['provider']

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=['--ec2-region'],
        scope=locals())

    if provider == 'ec2':
        cluster = ec2.get_cluster(
            cluster_name=cluster_name,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
    else:
        raise UnsupportedProviderError(provider)

    if not assume_yes:
        cluster.print()
        click.confirm(
            text="Are you sure you want to destroy this cluster?",
            abort=True)

    logger.info("Destroying {c}...".format(c=cluster.name))
    cluster.destroy()


@cli.command()
@click.argument('cluster-name', required=False)
@click.option('--master-hostname-only', is_flag=True, default=False)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
@click.pass_context
def describe(
        cli_context,
        cluster_name,
        master_hostname_only,
        ec2_region,
        ec2_vpc_id):
    """
    Describe an existing cluster.

    Leave out the cluster name to find all Flintrock-managed clusters.

    The output of this command is both human- and machine-friendly. Full cluster
    descriptions are output in YAML.
    """
    provider = cli_context.obj['provider']
    search_area = ""

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=['--ec2-region'],
        scope=locals())

    if cluster_name:
        cluster_names = [cluster_name]
    else:
        cluster_names = []

    if provider == 'ec2':
        search_area = "in region {r}".format(r=ec2_region)
        clusters = ec2.get_clusters(
            cluster_names=cluster_names,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
    else:
        raise UnsupportedProviderError(provider)

    if cluster_name:
        cluster = clusters[0]
        if master_hostname_only:
            logger.info(cluster.master_host)
        else:
            cluster.print()
    else:
        if master_hostname_only:
            for cluster in sorted(clusters, key=lambda x: x.name):
                logger.info("{}: {}".format(cluster.name, cluster.master_host))
        else:
            logger.info("Found {n} cluster{s}{space}{search_area}.".format(
                n=len(clusters),
                s='' if len(clusters) == 1 else 's',
                space=' ' if search_area else '',
                search_area=search_area))
            if clusters:
                logger.info('---')
                for cluster in sorted(clusters, key=lambda x: x.name):
                    cluster.print()


# TODO: Provide different command or option for going straight to Spark Shell. (?)
@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
# TODO: Move identity-file to global, non-provider-specific option. (?)
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-user')
@click.pass_context
def login(cli_context, cluster_name, ec2_region, ec2_vpc_id, ec2_identity_file, ec2_user):
    """
    Login to the master of an existing cluster.
    """
    provider = cli_context.obj['provider']

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=[
            '--ec2-region',
            '--ec2-identity-file',
            '--ec2-user'],
        scope=locals())

    check_external_dependency('ssh')

    if provider == 'ec2':
        cluster = ec2.get_cluster(
            cluster_name=cluster_name,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
        user = ec2_user
        identity_file = ec2_identity_file
    else:
        raise UnsupportedProviderError(provider)

    # TODO: Check that master up first and error out cleanly if not
    #       via ClusterInvalidState.
    cluster.login(user=user, identity_file=identity_file)


@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
# TODO: Move identity-file to global, non-provider-specific option. (?)
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-user')
@click.pass_context
def start(cli_context, cluster_name, ec2_region, ec2_vpc_id, ec2_identity_file, ec2_user):
    """
    Start an existing, stopped cluster.
    """
    provider = cli_context.obj['provider']

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=[
            '--ec2-region',
            '--ec2-identity-file',
            '--ec2-user'],
        scope=locals())

    if provider == 'ec2':
        cluster = ec2.get_cluster(
            cluster_name=cluster_name,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
        user = ec2_user
        identity_file = ec2_identity_file
    else:
        raise UnsupportedProviderError(provider)

    cluster.start_check()
    logger.info("Starting {c}...".format(c=cluster_name))
    cluster.start(user=user, identity_file=identity_file)


@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.pass_context
def stop(cli_context, cluster_name, ec2_region, ec2_vpc_id, assume_yes):
    """
    Stop an existing, running cluster.
    """
    provider = cli_context.obj['provider']

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=['--ec2-region'],
        scope=locals())

    if provider == 'ec2':
        cluster = ec2.get_cluster(
            cluster_name=cluster_name,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
    else:
        raise UnsupportedProviderError(provider)

    cluster.stop_check()

    if not assume_yes:
        cluster.print()
        click.confirm(
            text="Are you sure you want to stop this cluster?",
            abort=True)

    logger.info("Stopping {c}...".format(c=cluster_name))
    cluster.stop()
    logger.info("{c} is now stopped.".format(c=cluster_name))


@cli.command(name='add-slaves')
@click.argument('cluster-name')
@click.option('--num-slaves', type=click.IntRange(min=1), required=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-user')
@click.option('--ec2-spot-price', type=float)
@click.option('--ec2-spot-request-duration', default='7d',
              help="Duration a spot request is valid (e.g. 3d 2h 1m).")
@click.option('--ec2-min-root-ebs-size-gb', type=int, default=30)
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.option('--ec2-tag', 'ec2_tags',
              callback=ec2.cli_validate_tags,
              multiple=True,
              help="Additional tags (e.g. 'Key,Value') to assign to the instances. "
                   "You can specify this option multiple times.")
@click.pass_context
def add_slaves(
        cli_context,
        cluster_name,
        num_slaves,
        ec2_region,
        ec2_vpc_id,
        ec2_identity_file,
        ec2_user,
        ec2_spot_price,
        ec2_spot_request_duration,
        ec2_min_root_ebs_size_gb,
        ec2_tags,
        assume_yes):
    """
    Add slaves to an existing cluster.

    Flintrock will configure new slaves based on information queried
    automatically from the master.
    """
    provider = cli_context.obj['provider']

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=[
            '--ec2-region',
            '--ec2-identity-file',
            '--ec2-user'],
        scope=locals())

    if provider == 'ec2':
        cluster = ec2.get_cluster(
            cluster_name=cluster_name,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
        user = ec2_user
        identity_file = ec2_identity_file
        provider_options = {
            'min_root_ebs_size_gb': ec2_min_root_ebs_size_gb,
            'spot_price': ec2_spot_price,
            'spot_request_duration': ec2_spot_request_duration,
            'tags': ec2_tags
        }
    else:
        raise UnsupportedProviderError(provider)

    if cluster.num_masters == 0:
        raise Error(
            "Cannot add slaves to cluster '{c}' since it does not "
            "appear to have a master."
            .format(
                c=cluster_name))

    cluster.load_manifest(
        user=user,
        identity_file=identity_file)
    cluster.add_slaves_check()

    if provider == 'ec2':
        cluster.add_slaves(
            user=user,
            identity_file=identity_file,
            num_slaves=num_slaves,
            assume_yes=assume_yes,
            **provider_options)


@cli.command(name='remove-slaves')
@click.argument('cluster-name')
@click.option('--num-slaves', type=click.IntRange(min=1), required=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
@click.option('--ec2-user')
@click.option('--ec2-identity-file',
              type=click.Path(exists=True, dir_okay=False),
              help="Path to SSH .pem file for accessing nodes.")
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.pass_context
def remove_slaves(
        cli_context,
        cluster_name,
        num_slaves,
        ec2_region,
        ec2_vpc_id,
        ec2_user,
        ec2_identity_file,
        assume_yes):
    """
    Remove slaves from an existing cluster.
    """
    provider = cli_context.obj['provider']

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=[
            '--ec2-region',
            '--ec2-user',
            '--ec2-identity-file'],
        scope=locals())

    if provider == 'ec2':
        cluster = ec2.get_cluster(
            cluster_name=cluster_name,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
        user = ec2_user
        identity_file = ec2_identity_file
    else:
        raise UnsupportedProviderError(provider)

    if num_slaves > cluster.num_slaves:
        logger.warning(
            "Warning: Cluster has {c} slave{cs}. "
            "You asked to remove {n} slave{ns}."
            .format(
                c=cluster.num_slaves,
                cs='' if cluster.num_slaves == 1 else 's',
                n=num_slaves,
                ns='' if num_slaves == 1 else 's'))
        num_slaves = cluster.num_slaves

    if not assume_yes:
        cluster.print()
        click.confirm(
            text=("Are you sure you want to remove {n} slave{s} from this cluster?"
                  .format(
                      n=num_slaves,
                      s='' if num_slaves == 1 else 's')),
            abort=True)

    logger.info("Removing {n} slave{s}..."
                .format(
                    n=num_slaves,
                    s='' if num_slaves == 1 else 's'))
    cluster.remove_slaves(
        user=user,
        identity_file=identity_file,
        num_slaves=num_slaves)


@cli.command(name='run-command')
@click.argument('cluster-name')
@click.argument('command', nargs=-1)
@click.option('--master-only', help="Run on the master only.", is_flag=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
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
        ec2_vpc_id,
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
    provider = cli_context.obj['provider']

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=[
            '--ec2-region',
            '--ec2-identity-file',
            '--ec2-user'],
        scope=locals())

    if provider == 'ec2':
        cluster = ec2.get_cluster(
            cluster_name=cluster_name,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
        user = ec2_user
        identity_file = ec2_identity_file
    else:
        raise UnsupportedProviderError(provider)

    cluster.run_command_check()

    logger.info("Running command on {target}...".format(
        target="master only" if master_only else "cluster"))

    cluster.run_command(
        command=command,
        master_only=master_only,
        user=user,
        identity_file=identity_file)


@cli.command(name='copy-file')
@click.argument('cluster-name')
@click.argument('local_path', type=click.Path(exists=True, dir_okay=False))
@click.argument('remote_path', type=click.Path())
@click.option('--master-only', help="Copy to the master only.", is_flag=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-vpc-id', default='', help="Leave empty for default VPC.")
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
        ec2_vpc_id,
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
    provider = cli_context.obj['provider']

    option_requires(
        option='--provider',
        conditional_value='ec2',
        requires_all=[
            '--ec2-region',
            '--ec2-identity-file',
            '--ec2-user'],
        scope=locals())

    # We assume POSIX for the remote path since Flintrock
    # only supports clusters running CentOS / Amazon Linux.
    if not posixpath.basename(remote_path):
        remote_path = posixpath.join(remote_path, os.path.basename(local_path))

    if provider == 'ec2':
        cluster = ec2.get_cluster(
            cluster_name=cluster_name,
            region=ec2_region,
            vpc_id=ec2_vpc_id)
        user = ec2_user
        identity_file = ec2_identity_file
    else:
        raise UnsupportedProviderError(provider)

    cluster.copy_file_check()

    if not assume_yes and not master_only:
        file_size_bytes = os.path.getsize(local_path)
        num_nodes = len(cluster.slave_ips) + 1  # TODO: cluster.num_nodes
        total_size_bytes = file_size_bytes * num_nodes

        if total_size_bytes > 10 ** 6:
            logger.warning("WARNING:")
            logger.warning(
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

    logger.info("Copying file to {target}...".format(
        target="master only" if master_only else "cluster"))

    cluster.copy_file(
        local_path=local_path,
        remote_path=remote_path,
        master_only=master_only,
        user=user,
        identity_file=identity_file)


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
    service_configs = {}

    if 'services' in config:
        for service in config['services']:
            if config['services'][service]:
                service_configs.update(
                    {service + '_' + k: v for (k, v) in config['services'][service].items()})

    ec2_configs = {
        'ec2_' + k: v for (k, v) in config['providers']['ec2'].items()}

    click_map = {
        'launch': dict(
            list(config['launch'].items())
            + list(ec2_configs.items())
            + list(service_configs.items())),
        'describe': ec2_configs,
        'destroy': ec2_configs,
        'login': ec2_configs,
        'start': ec2_configs,
        'stop': ec2_configs,
        'add-slaves': ec2_configs,
        'remove-slaves': ec2_configs,
        'run-command': ec2_configs,
        'copy-file': ec2_configs,
    }

    return click_map


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
        logger.info("Initializing config file from template...")
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        shutil.copyfile(
            src=os.path.join(THIS_DIR, 'config.yaml.template'),
            dst=config_file)
        os.chmod(config_file, mode=0o644)

    ret = click.launch(config_file, locate=locate)

    if ret != 0:
        raise Error(
            "Flintrock could not launch an application to {action} "
            "the config file at '{location}'. You may want to manually "
            "find and edit this file."
            .format(
                action="locate" if locate else "edit",
                location=config_file
            )
        )


def flintrock_is_in_development_mode() -> bool:
    """
    Check if Flintrock was installed in development mode.

    Use this function to toggle behavior that only Flintrock developers should
    see.
    """
    # This esoteric technique was pulled from pip.
    # See: https://github.com/pypa/pip/pull/3258/files#diff-ab583908279e865537dec218246edcfcR310
    for path_item in sys.path:
        egg_link = os.path.join(path_item, 'Flintrock.egg-link')
        if os.path.isfile(egg_link):
            return True
    else:
        return False


def set_open_files_limit(desired_limit):
    """
    On POSIX systems, set the open files limit to the desired number, unless
    it is already equal to or higher than that.

    Setting a high limit enables Flintrock to launch or interact with really
    large clusters.

    Background discussion: https://github.com/nchammas/flintrock/issues/81
    """
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)

    if soft_limit < desired_limit:
        if desired_limit > hard_limit:
            warnings.warn(
                "Flintrock cannot set the open files limit to {desired} "
                "because the OS hard limit is {hard}. Going with {hard}. "
                "You may have problems launching or interacting with "
                "really large clusters."
                .format(
                    desired=desired_limit,
                    hard=hard_limit),
                category=RuntimeWarning,
                stacklevel=2)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (min(desired_limit, hard_limit), hard_limit))


def check_external_dependency(executable_name: str):
    if shutil.which(executable_name) is None:
        raise Error(
            "Error: Flintrock could not find '{executable}' on your PATH. "
            "Flintrock needs this executable to carry out the operation you "
            "requested. Please install it and try again."
            .format(
                executable=executable_name
            )
        )


def main() -> int:
    # Starting in Python 3.7, deprecation warnings are shown by default. We
    # don't want to show these to end-users.
    # See: https://docs.python.org/3/library/warnings.html#default-warning-filter
    if not flintrock_is_in_development_mode():
        warnings.simplefilter(action='ignore', category=DeprecationWarning)

    set_open_files_limit(4096)

    try:
        try:
            # We pass in obj so we can add attributes to it, like provider, which
            # get shared by all commands.
            # See: http://click.pocoo.org/6/api/#click.Context
            cli(obj={})
        except botocore.exceptions.NoCredentialsError:
            raise Error(
                "Flintrock could not find your AWS credentials. "
                "You can fix this by providing your credentials "
                "via environment variables or by creating a shared "
                "credentials file.\n"
                "For more information see:\n"
                "  * https://boto3.readthedocs.io/en/latest/guide/configuration.html#environment-variables\n"
                "  * https://boto3.readthedocs.io/en/latest/guide/configuration.html#shared-credentials-file"
            )
    except NothingToDo as e:
        print(e)
        return 0
    except UsageError as e:
        print(e, file=sys.stderr)
        return 2
    except Error as e:
        print(e, file=sys.stderr)
        return 1
