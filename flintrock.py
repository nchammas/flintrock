"""
Flintrock

A command-line tool and library for launching Apache Spark clusters.

Major TODOs:
    * Handling of exceptions / reporting of issues during cluster launch.
        - Spark install goes wrong
        - Spark version is invalid
        - Current exception output is quite ugly. Related to thread executor / asyncio.
    * "Fix" Hadoop 2.6 S3 setup by installing appropriate Hadoop libraries
      See: https://issues.apache.org/jira/browse/SPARK-7442
    * ClusterInfo namedtuple -> FlintrockCluster class
        - Platform-specific (e.g. EC2) implementations of class add methods to
          stop, start, describe (with YAML output) etc. clusters
        - Implement method that takes cluster name and returns FlintrockCluster
    * Support submit command for Spark applications. Like a wrapper around spark-submit. (?)
    * ext4/disk setup.
    * EBS volume setup.
    * Check that EC2 enhanced networking is enabled.
    * Upgrade to boto3: http://boto3.readthedocs.org/en/latest/
        - What are the long-term benefits?

Other TODOs:
    * Support for spot instances.
        - Show wait reason (capcity oversubscribed, price too low, etc.).
    * Instance type <-> AMI type validation/lookup.
        - Maybe this can be automated.
        - Otherwise have a separate YAML file with this info.
        - Maybe support HVM only. AWS seems to position it as the future.
        - Show friendly error when user tries to launch PV instance type.
    * Move defaults like Spark version to external file. (Maybe to existing user defaults file?)
        - Poll external resource if default is not specified in file.
            (e.g. check GitHub tags for latest Spark version) (?)
    * Use IAM roles to launch instead of AWS keys.
    * Setup and teardown VPC, routes, gateway, etc. from scratch.
    * Use SSHAgent instead of .pem files (?).
    * Automatically replace failed instances during launch, perhaps up to a
      certain limit (1-2 instances).
    * Upgrade check -- Is a newer version of Flintrock available on PyPI?
    * Credits command, for crediting contributors. (?)

Distant future:
    * Local provider
    * [probably-not] Allow master and slaves to be different (spot, instance type, etc).
"""

import os
import errno
import sys
import pprint
import time
from datetime import datetime

# External modules.
import boto
import boto.ec2
import click
import yaml

# Flintrock modules.
from flint.providers.ec2 import AmazonEc2Provider

_SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULT_SPARK_VERSION = '1.5.0'

def get_default_modules(install_spark = True):
    modules = []

    if install_spark:
        from flint.modules.spark import Spark
        spark = Spark(version=DEFAULT_SPARK_VERSION)
        modules += [spark]
    return modules

@click.group()
@click.option('--config', default=_SCRIPT_DIR + '/config.yaml')
@click.option('--provider', default='ec2', type=click.Choice(['ec2']))
@click.version_option(version='dev')  # TODO: Replace with setuptools auto-detect.
@click.pass_context
def cli(cli_context, config, provider):
    """
    Flintrock

    A command-line tool and library for launching Apache Spark clusters.
    """
    cli_context.obj['provider'] = provider

    if os.path.exists(config):
        with open(config) as f:
            raw_config = yaml.safe_load(f)
            config_map = normalize_keys(config_to_click(raw_config))

        cli_context.default_map = config_map
    else:
        if config != (_SCRIPT_DIR + '/config.yaml'):
            raise FileNotFoundError(errno.ENOENT, 'No such file or directory', config)


# @timeit  # Why doesn't this work?
# TODO: Required EC2 parameters shouldn't be required for non-EC2 providers.
#       Click doesn't support this kind of flow directly.
#       See: https://github.com/mitsuhiko/click/issues/257
@cli.command()
@click.argument('cluster-name')
@click.option('--num-slaves', type=int, required=True)
@click.option('--install-spark/--no-install-spark', default=True)
@click.option('--spark-version', default=DEFAULT_SPARK_VERSION, show_default=True)
@click.option('--ec2-key-name')
@click.option('--ec2-identity-file', help="Path to SSH .pem file for accessing nodes.")
@click.option('--ec2-instance-type', default='m3.medium', show_default=True)
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-availability-zone')
@click.option('--ec2-ami')
@click.option('--ec2-spot-price', type=float)
@click.option('--ec2-vpc-id')
@click.option('--ec2-subnet-id')
@click.option('--ec2-placement-group')
@click.option('--ec2-tenancy', default='default')
@click.option('--ec2-ebs-optimized/--no-ec2-ebs-optimized', default=False)
@click.option('--ec2-instance-initiated-shutdown-behavior', default='stop',
              type=click.Choice(['stop', 'terminate']))
@click.pass_context
def launch(
        cli_context,
        cluster_name, num_slaves,
        install_spark,
        spark_version,
        ec2_key_name,
        ec2_identity_file,
        ec2_instance_type,
        ec2_region,
        ec2_availability_zone,
        ec2_ami,
        ec2_spot_price,
        ec2_vpc_id,
        ec2_subnet_id,
        ec2_placement_group,
        ec2_tenancy,
        ec2_ebs_optimized,
        ec2_instance_initiated_shutdown_behavior):
    """
    Launch a new cluster.
    """

    if cli_context.obj['provider'] == 'ec2':
        provider = AmazonEc2Provider(cluster_name=cluster_name,
                                     modules=get_default_modules(),
                                     region=ec2_region)
        return provider.launch_cluster(
            num_slaves=num_slaves,
            key_name=ec2_key_name,
            identity_file=ec2_identity_file,
            instance_type=ec2_instance_type,
            region=ec2_region,
            availability_zone=ec2_availability_zone,
            ami=ec2_ami,
            spot_price=ec2_spot_price,
            vpc_id=ec2_vpc_id,
            subnet_id=ec2_subnet_id,
            placement_group=ec2_placement_group,
            tenancy=ec2_tenancy,
            ebs_optimized=ec2_ebs_optimized,
            instance_initiated_shutdown_behavior=ec2_instance_initiated_shutdown_behavior)
    else:
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


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
        provider = AmazonEc2Provider(cluster_name=cluster_name,
                                     modules=get_default_modules(),
                                     region=ec2_region)
        provider.destroy_cluster(assume_yes=assume_yes,
                                 region=ec2_region)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))



def add_slaves(provider, cluster_name, num_slaves, provider_options):
    # Need concept of cluster state so we can add slaves with the same config.
    # Otherwise we must ask unreliable user to respecify slave config.
    pass



def remove_slaves(provider, cluster_name, num_slaves, provider_options, assume_yes=False):
    pass


@cli.command()
@click.argument('cluster-name', required=False)
@click.option('--master-hostname-only', is_flag=True, default=False)
# TODO: EC2 region is gloal to all EC2 operations. Can that be captured somehow?
# TODO: Required EC2 options should be required only when the EC2 provider is selected.
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
        provider = AmazonEc2Provider(cluster_name=cluster_name,
                                     modules=get_default_modules(),
                                     region=ec2_region)
        provider.describe_cluster(
            master_hostname_only=master_hostname_only,
            region=ec2_region)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))





# TODO: Provide different command or option for going straight to Spark Shell.
@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
# TODO: Move identity-file to global, non-provider-specific option. (?)
@click.option('--ec2-identity-file', help="Path to .pem file for SSHing into nodes.")
@click.option('--ssh-tunnel', default=None,
              help="Set up ssh port forwarding when you login to the cluster.  " +
              "This provides a convenient alternative to connecting to iPython " +
              "notebook over an open port using SSL.  You must supply an argument " +
              "of the form \"local_port:remote_port\".")
@click.pass_context
def login(cli_context, cluster_name, ec2_region, ec2_identity_file, ssh_tunnel):
    """
    Login to the master of an existing cluster.
    """
    if cli_context.obj['provider'] == 'ec2':
        provider = AmazonEc2Provider(cluster_name=cluster_name,
                                     modules=get_default_modules(),
                                     region=ec2_region)
        provider.login(
            region=ec2_region,
            identity_file=ec2_identity_file,
            ssh_tunnel_ports=ssh_tunnel)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--ec2-identity-file', help="Path to SSH .pem file for accessing nodes.")
@click.pass_context
def start(cli_context, cluster_name, ec2_identity_file, ec2_region):
    """
    Start an existing, stopped cluster.
    """

    if cli_context.obj['provider'] == 'ec2':
        provider = AmazonEc2Provider(cluster_name=cluster_name,
                                     modules=get_default_modules(),
                                     region=ec2_region)
        provider.start_cluster(identity_file = ec2_identity_file)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))



@cli.command()
@click.argument('cluster-name')
@click.option('--ec2-region', default='us-east-1', show_default=True)
@click.option('--assume-yes/--no-assume-yes', default=False)
@click.option('--wait-for-confirmation/--no-wait-for-confirmation', default=False)
@click.pass_context
def stop(cli_context, cluster_name, ec2_region, assume_yes, wait_for_confirmation):
    """
    Stop an existing, running cluster.
    """

    modules = get_default_modules()
    if cli_context.obj['provider'] == 'ec2':
        provider = AmazonEc2Provider(cluster_name=cluster_name,
                                     modules=get_default_modules(),
                                     region=ec2_region)
        provider.stop_cluster(assume_yes=assume_yes, wait_for_confirmation=wait_for_confirmation)
    else:
        # TODO: Create UnsupportedProviderException. (?)
        raise Exception("This provider is not supported: {p}".format(p=cli_context.obj['provider']))


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
    ec2_configs = {
        'ec2-' + k: v for (k, v) in config['providers']['ec2'].items()}

    click = {
        'launch': dict(
            list(config['launch'].items()) + list(ec2_configs.items())),
        'start': dict(
            list(config['launch'].items()) + list(ec2_configs.items())),
        'describe': ec2_configs,
        'login': ec2_configs
    }

    return click


if __name__ == "__main__":
    cli(obj={})
