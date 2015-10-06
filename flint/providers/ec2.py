
import sys
import time
import shlex
from collections import namedtuple

import click
import boto
import boto.ec2
import urllib.request

# Flintrock modules.
from ..util import timeit
from ..ssh import generate_ssh_key_pair
from ..ssh import ssh_check_output
from ..ssh import ssh

# ---------------------------------------------------------------------------------
#                               HELPER FUNCTIONS
# ---------------------------------------------------------------------------------


# boto is not thread-safe so each task needs to create its own connection.
# Reference, from boto's primary maintainer: http://stackoverflow.com/a/19542645/
def provision_ec2_node(*,
        modules,
        host,
        identity_file,
        cluster_info):
    """
    Connect to a freshly launched EC2 instance, set it up for SSH access, and
    install the specified modules.

    This function is intended to be called on all cluster nodes in parallel.

    No master- or slave-specific logic should be in this method.
    """
    import paramiko
    import socket
    with paramiko.client.SSHClient() as client:
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

        while True:
            try:
                client.connect(
                    username="ec2-user",
                    hostname=host,
                    key_filename=identity_file,
                    timeout=3)
                print("[{h}] SSH online.".format(h=host))
                break
            except socket.timeout as e:
                time.sleep(5)
            except socket.error as e:
                if e.errno != 61:
                    raise
                time.sleep(5)

        # --- SSH is now available. ---
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

        # --- Install Modules. ---
        for module in modules:
            module.install(
                ssh_client=client,
                cluster_info=cluster_info)

# ---------------------------------------------------------------------------------
#                              ClusterInfo Class
# ---------------------------------------------------------------------------------

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
        'master_host',
        'slave_hosts',
        'spark_scratch_dir',
        'spark_master_opts'
    ])

# ---------------------------------------------------------------------------------
#                            EC2 Provider Methods
# ---------------------------------------------------------------------------------

@timeit
def launch(*,
        cluster_name, num_slaves, modules,
        key_name, identity_file,
        instance_type,
        region,
        availability_zone,
        ami,
        spot_price=None,
        vpc_id, subnet_id, placement_group,
        tenancy="default", ebs_optimized=False,
        instance_initiated_shutdown_behavior="stop"):
    """
    Launch a fully functional cluster on EC2 with the specified configuration
    and installed modules.
    """
    connection = boto.ec2.connect_to_region(region_name=region)

    def get_or_create_security_groups(cluster_name, vpc_id) -> 'List[boto.ec2.securitygroup.SecurityGroup]':
        """
        If they do not already exist, create all the security groups needed for a
        Flintrock cluster.
        """
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
        flintrock_group = next((sg for sg in search_results if sg.name == flintrock_group_name), None)
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
            SecurityGroupRule(
                ip_protocol='tcp',
                from_port=22,
                to_port=22,
                cidr_ip=flintrock_client_cidr,
                src_group=None),
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
                flintrock_group.authorize(**vars(rule))
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
                cluster_group.authorize(**vars(rule))
            except boto.exception.EC2ResponseError as e:
                if e.error_code != 'InvalidPermission.Duplicate':
                    print("Error adding rule: {r}".format(r=rule))
                    raise

        return [flintrock_group, cluster_group]

    security_groups = get_or_create_security_groups(cluster_name=cluster_name, vpc_id=vpc_id)

    try:
        reservation = connection.run_instances(
            image_id=ami,
            min_count=(num_slaves + 1),
            max_count=(num_slaves + 1),
            key_name=key_name,
            instance_type=instance_type,
            placement=availability_zone,
            security_group_ids=[sg.id for sg in security_groups],
            subnet_id=subnet_id,
            placement_group=placement_group,
            tenancy=tenancy,
            ebs_optimized=ebs_optimized,
            instance_initiated_shutdown_behavior=instance_initiated_shutdown_behavior)

        time.sleep(10)  # AWS metadata eventual consistency tax.

        while True:
            for instance in reservation.instances:
                if instance.state == 'running':
                    continue
                else:
                    instance.update()
                    time.sleep(3)
                    break
            else:
                print("All {c} instances now running.".format(
                    c=len(reservation.instances)))
                break

        master_instance = reservation.instances[0]
        slave_instances = reservation.instances[1:]

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
            master_host=master_instance.public_dns_name,
            slave_hosts=[instance.public_dns_name for instance in slave_instances],
            spark_scratch_dir='/mnt/spark',
            spark_master_opts="")

        # TODO: Abstract away. No-one wants to see this async shite here.
        import asyncio
        import functools
        loop = asyncio.get_event_loop()

        tasks = []
        for instance in reservation.instances:
            # TODO: Use parameter names for run_in_executor() once Python 3.4.4 is released.
            #       Until then, we leave them out to maintain compatibility across Python 3.4
            #       and 3.5.
            # See: http://stackoverflow.com/q/32873974/
            task = loop.run_in_executor(
                None,
                functools.partial(
                    provision_ec2_node,
                    modules=modules,
                    host=instance.ip_address,
                    identity_file=identity_file,
                    cluster_info=cluster_info))
            tasks.append(task)
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()

        print("All {c} instances provisioned.".format(
            c=len(reservation.instances)))

        # --- This stuff here runs after all the nodes are provisioned. ---
        import paramiko
        with paramiko.client.SSHClient() as client:
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

            client.connect(
                username="ec2-user",
                hostname=master_instance.public_dns_name,
                key_filename=identity_file,
                timeout=3)

            for module in modules:
                module.configure_master(
                    ssh_client=client,
                    cluster_info=cluster_info)

            # Login to the master for manual inspection.
            # TODO: Move to master_login() method.
            # ret = subprocess.call(
            #     """
            #     set -x
            #     ssh -o "StrictHostKeyChecking=no" \
            #         -i {identity_file} \
            #         ec2-user@{host}
            #     """.format(
            #         identity_file=shlex.quote(identity_file),
            #         host=shlex.quote(master_instance.public_dns_name)),
            #     shell=True)

    except KeyboardInterrupt as e:
        print("Exiting...")
        sys.exit(1)
    # finally:
    #     print("Terminating all {c} instances...".format(
    #         c=len(reservation.instances)))

    #     for instance in reservation.instances:
    #         instance.terminate()



# assume_yes defaults to True here for library use (as opposed to command-line use).
def destroy(*, cluster_name, assume_yes=True, region):
    connection = boto.ec2.connect_to_region(region_name=region)

    cluster_instances = connection.get_only_instances(
        filters={
            'instance.group-name': 'flintrock-' + cluster_name
        })

    # Should this be an error? ClusterNotFound exception?
    if not cluster_instances:
        print("No such cluster.")
        sys.exit(0)
        # Style: Should everything else be under an else: block?

    if not assume_yes:
        print_cluster_info(
            cluster_name=cluster_name,
            cluster_instances=cluster_instances)

        print('---')

        click.confirm(
            text="Are you sure you want to destroy this cluster?",
            abort=True)

    # TODO: Figure out if we want to use "node" instead of "instance" when
    #       communicating with the user, even if we're talking about doing things
    #       to EC2 instances. Spark docs definitely favor "node".
    print("Terminating {c} instances...".format(c=len(cluster_instances)))
    for instance in cluster_instances:
        instance.terminate()

    # TODO: Destroy cluster security group. We're not reusing it.

def add_slaves(cluster_name, num_slaves, identity_file):
    pass

def remove_slaves(cluster_name, num_slaves, assume_yes=True):
    pass

def get_cluster_state(cluster_instances: list) -> str:
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

def print_cluster_info(cluster_name: str, cluster_instances: list):
    """
    Print information about an EC2 cluster to screen in a YAML-compatible format.

    This is the current solution until cluster methods are centralized under a
    FlintrockCluster class, or something similar.
    """
    print(cluster_name + ':')
    print('  state: {s}'.format(s=get_cluster_state(cluster_instances=cluster_instances)))
    print('  node-count: {nc}'.format(nc=len(cluster_instances)))

    if get_cluster_state(cluster_instances=cluster_instances) == 'running':
        print('\n    - '.join(['  nodes:'] + [i.public_dns_name for i in cluster_instances]))

def describe(*, cluster_name, master_hostname_only=False, region):
    connection = boto.ec2.connect_to_region(region_name=region)

    cluster_instances = connection.get_only_instances(
        filters={
            'instance.group-name': 'flintrock-' + cluster_name if cluster_name else 'flintrock',
            'instance-state-name' : 'running'
        })

    # TODO: Capture this in some reusable method that gets info about a bunch of
    #       Flintrock clusters and returns a list of FlintrockCluster objects.
    #
    #       Then, maybe just serialize that list to screen using YAML.
    #       You'll have to deal with PyYAML's inability to customize the output
    #       order of the keys.
    #
    #       See: https://issues.apache.org/jira/browse/SPARK-5629?focusedCommentId=14325346#comment-14325346
    #       Add provider-specific information like EC2 region.
    import itertools
    security_groups = itertools.chain.from_iterable([i.groups for i in cluster_instances])
    security_group_names = {g.name for g in security_groups if g.name.startswith('flintrock-')}
    cluster_names = [n.replace('flintrock-', '', 1) for n in security_group_names]

    print("{n} cluster{s} found.".format(
        n=len(cluster_names),
        s='' if len(cluster_names) == 1 else 's'))

    if cluster_names:
        print('---')

        for cluster_name in sorted(cluster_names):
            filtered_instances = []

            for instance in cluster_instances:
                if ('flintrock-' + cluster_name) in {g.name for g in instance.groups}:
                    filtered_instances.append(instance)

            print_cluster_info(
                cluster_name=cluster_name,
                cluster_instances=filtered_instances)


def login(cluster_name, region, identity_file):
    connection = boto.ec2.connect_to_region(region_name=region)

    master_instance = next(iter(
        connection.get_only_instances(
            filters={
                'instance.group-name': 'flintrock-' + cluster_name,
                'tag:flintrock-role': 'master',
                'instance-state-name' : 'running'
            })),
                           None)

    if master_instance:
        ssh(
            host=master_instance.public_dns_name,
            identity_file=identity_file)
    else:
        # TODO: Custom MasterNotFound exception. (?)
        raise Exception(
            "Could not find a master for a cluster named '{c}' in the {r} region.".format(
                c=cluster_name,
                r=region))

def start(cluster_name, region):
    # TODO: Replace this with a common get_cluster_info() method.
    connection = boto.ec2.connect_to_region(region_name=region)

    cluster_instances = connection.get_only_instances(
        filters={
            'instance.group-name': 'flintrock-' + cluster_name,
            'instance-state-name' : 'stopped'
        })

    # Should this be an error? ClusterNotFound exception?
    if not cluster_instances:
        print("No such cluster.")
        sys.exit(0)
        # Style: Should everything else be under an else: block?

    print("Starting {c} instances...".format(c=len(cluster_instances)))
    for instance in cluster_instances:
        instance.start()

    while True:
        for instance in cluster_instances:
            if instance.state == 'running':
                continue
            else:
                instance.update()
                time.sleep(3)
                break
        else:
            print("{c} is now running.".format(c=cluster_name))
            break



def stop(cluster_name, region, assume_yes=True, wait_for_confirmation = False):
    # TODO: Replace this with a common get_cluster_info() method.
    connection = boto.ec2.connect_to_region(region_name=region)

    cluster_instances = connection.get_only_instances(
        filters={
            'instance.group-name': 'flintrock-' + cluster_name,
            'instance-state-name' : 'running'
        })

    # Should this be an error? ClusterNotFound exception?
    if not cluster_instances:
        print("No such cluster.")
        sys.exit(0)
        # Style: Should everything else be under an else: block?

    if not assume_yes:
        print_cluster_info(
            cluster_name=cluster_name,
            cluster_instances=cluster_instances)

        print('---')

        click.confirm(
            text="Are you sure you want to stop this cluster?",
            abort=True)

    print("Stopping {c} instances...".format(c=len(cluster_instances)))
    for instance in cluster_instances:
        instance.stop()

    # Fully stopping an instance can sometimes take a very long time, and the user rarely needs
    # confirmation that the instance has stopped.  Only wait for a "stopped" status if the user
    # explicitly requests it.
    if wait_for_confirmation:
        desired_state = 'stopped'
    else:
        desired_state = 'stopping'


    while True:
        for instance in cluster_instances:
            if instance.state == desired_state:
                continue
            else:
                instance.update()
                time.sleep(3)
                break
        else:
            print("{c} is now stopped.".format(c=cluster_name))
            break
