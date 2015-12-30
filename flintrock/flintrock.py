import os
import posixpath
import errno
import sys
import shutil
import warnings

# External modules
import click
import yaml

# Flintrock modules
from . import ec2
from .exceptions import (
    UsageError,
    UnsupportedProviderError,
    NothingToDo,
    ClusterAlreadyExists,
    ClusterInvalidState)
from flintrock import __version__
from .services import HDFS, Spark  # TODO: Remove this dependency.

THIS_DIR = os.path.dirname(os.path.realpath(__file__))


def get_config_file() -> str:
    """
    Get the path to Flintrock's default configuration file.
    """
    config_dir = click.get_app_dir(app_name='Flintrock')
    config_file = os.path.join(config_dir, 'config.yaml')
    return config_file


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
@click.option('--install-hdfs/--no-install-hdfs', default=False)
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
@click.option('--assume-yes/--no-assume-yes', default=False)
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
        cluster_name,
        num_slaves,
        install_hdfs,
        hdfs_version,
        install_spark,
        spark_version,
        spark_git_commit,
        spark_git_repository,
        assume_yes,
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
    services = []

    if install_hdfs:
        if not hdfs_version:
            raise UsageError(
                "Error: Cannot install HDFS. Missing option \"--hdfs-version\".")
        hdfs = HDFS(version=hdfs_version)
        services += [hdfs]
    if install_spark:
        if ((not spark_version and not spark_git_commit) or
                (spark_version and spark_git_commit)):
            # TODO: API for capturing option dependencies like this.
            raise UsageError(
                "Error: Cannot install Spark. Exactly one of \"--spark-version\" or "
                "\"--spark-git-commit\" must be specified.\n"
                "--spark-version: " + spark_version + "\n"
                "--spark-git-commit: " + spark_git_commit)
        else:
            if spark_version:
                spark = Spark(version=spark_version)
            elif spark_git_commit:
                print("Warning: Building Spark takes a long time. "
                      "e.g. 15-20 minutes on an m3.xlarge instance on EC2.")
                spark = Spark(git_commit=spark_git_commit,
                              git_repository=spark_git_repository)
            services += [spark]

    if cli_context.obj['provider'] == 'ec2':
        return ec2.launch(
            cluster_name=cluster_name,
            num_slaves=num_slaves,
            services=services,
            assume_yes=assume_yes,
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
        raise UnsupportedProviderError(cli_context.obj['provider'])


@cli.command()
@click.argument('cluster-name')
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.pass_context
def destroy(cli_context, cluster_name, assume_yes, ec2_region):
    """
    Destroy a cluster.
    """
    if cli_context.obj['provider'] == 'ec2':
        ec2.destroy(
            cluster_name=cluster_name,
            assume_yes=assume_yes,
            region=ec2_region)
    else:
        raise UnsupportedProviderError(cli_context.obj['provider'])


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
        ec2.describe(
            cluster_name=cluster_name,
            master_hostname_only=master_hostname_only,
            region=ec2_region)
    else:
        raise UnsupportedProviderError(cli_context.obj['provider'])


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
        ec2.login(
            cluster_name=cluster_name,
            region=ec2_region,
            identity_file=ec2_identity_file,
            user=ec2_user)
    else:
        raise UnsupportedProviderError(cli_context.obj['provider'])


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
        ec2.start(
            cluster_name=cluster_name,
            region=ec2_region,
            identity_file=ec2_identity_file,
            user=ec2_user)
    else:
        raise UnsupportedProviderError(cli_context.obj['provider'])


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
        ec2.stop(
            cluster_name=cluster_name,
            region=ec2_region,
            assume_yes=assume_yes)
    else:
        raise UnsupportedProviderError(cli_context.obj['provider'])


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
        ec2.run_command(
            cluster_name=cluster_name,
            command=command,
            master_only=master_only,
            region=ec2_region,
            identity_file=ec2_identity_file,
            user=ec2_user)
    else:
        raise UnsupportedProviderError(cli_context.obj['provider'])


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
        ec2.copy_file(
            cluster_name=cluster_name,
            local_path=local_path,
            remote_path=remote_path,
            master_only=master_only,
            region=ec2_region,
            identity_file=ec2_identity_file,
            user=ec2_user,
            assume_yes=assume_yes)
    else:
        raise UnsupportedProviderError(cli_context.obj['provider'])


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

    if 'modules' in config:
        print(
            "WARNING: The name `modules` is deprecated and will be removed "
            "in the next version of Flintrock.\n"
            "Please update your config file to use `services` instead of `modules`.\n"
            "You can do this by calling `flintrock configure`.")
        config['services'] = config['modules']

    if 'services' in config:
        for service in config['services']:
            if config['services'][service]:
                service_configs.update(
                    {service + '_' + k: v for (k, v) in config['services'][service].items()})

    ec2_configs = {
        'ec2_' + k: v for (k, v) in config['providers']['ec2'].items()}

    click_map = {
        'launch': dict(
            list(config['launch'].items()) +
            list(ec2_configs.items()) +
            list(service_configs.items())),
        'describe': ec2_configs,
        'destroy': ec2_configs,
        'login': ec2_configs,
        'start': ec2_configs,
        'stop': ec2_configs,
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
        print("Initializing config file from template...")
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        shutil.copyfile(
            src=os.path.join(THIS_DIR, 'config.yaml.template'),
            dst=config_file)
        os.chmod(config_file, mode=0o644)

    click.launch(config_file, locate=locate)


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


def main() -> int:
    if flintrock_is_in_development_mode():
        warnings.simplefilter(action='error', category=DeprecationWarning)

    try:
        # We pass in obj so we can add attributes to it, like provider, which
        # get shared by all commands.
        # See: http://click.pocoo.org/6/api/#click.Context
        cli(obj={})
    except NothingToDo as e:
        print(e)
        return 0
    except UsageError as e:
        print(e, file=sys.stderr)
        return 2
    except Exception as e:
        print(e, file=sys.stderr)
        return 1
