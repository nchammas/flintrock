import asyncio
import functools
import itertools
import json
import os
import shlex
import string
import sys
import time
import urllib.request
from collections import namedtuple
from datetime import datetime

# External modules
import boto
import boto.ec2
import click

# Flintrock modules
from .core import ClusterInfo
from .core import format_message, generate_ssh_key_pair
from .core import get_ssh_client, ssh_check_output, ssh
from .core import HDFS, Spark  # Used by start_ec2
from .core import provision_node, start_node, run_command_node, copy_file_node
from .exceptions import ClusterNotFound


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now().replace(microsecond=0)
        res = func(*args, **kwargs)
        end = datetime.now().replace(microsecond=0)
        print("{f} finished in {t}.".format(f=func.__name__, t=(end - start)))
        return res
    return wrapper


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
            description="Flintrock base group",
            vpc_id=vpc_id)

    # Rules for the client interacting with the cluster.
    flintrock_client_ip = (
        urllib.request.urlopen('http://checkip.amazonaws.com/')
        .read().decode('utf-8').strip())
    flintrock_client_cidr = '{ip}/32'.format(ip=flintrock_client_ip)

    # Modules should be responsible for registering what ports they want exposed.
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
        cluster_name,
        num_slaves,
        modules,
        assume_yes,
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

    try:
        security_groups = get_or_create_ec2_security_groups(
            cluster_name=cluster_name,
            vpc_id=vpc_id,
            region=region)
        block_device_map = get_ec2_block_device_map(
            ami=ami,
            region=region)
    except boto.exception.EC2ResponseError as e:
        if e.error_code == 'InvalidAMIID.NotFound':
            print("Error: Could not find {ami} in region {region}."
                  .format(ami=ami, region=region), file=sys.stderr)
            sys.exit(1)
        else:
            raise

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

# -- SPLIT HERE --

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
            if not assume_yes:
                yes = click.confirm(
                    text="Do you want to terminate the {c} instances created by this operation?"
                         .format(c=len(cluster_instances)),
                    err=True,
                    default=True)

            if assume_yes or yes:
                print("Terminating instances...", file=sys.stderr)
                connection.terminate_instances(
                    instance_ids=[instance.id for instance in cluster_instances])

        sys.exit(1)
    # finally:
    #     print("Terminating all {c} instances...".format(
    #         c=len(cluster_instances)))
    #     connection.terminate_instances(
    #         instance_ids=[instance.id for instance in cluster_instances])


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

    connection = boto.ec2.connect_to_region(region_name=region)

    # TODO: Centralize logic to get Flintrock base security group. (?)
    flintrock_base_group = connection.get_all_security_groups(groupnames=['flintrock'])
    # TODO: Is there a way to do this in one call? Do we need to throttle these calls?
    for instance in cluster_instances:
        connection.modify_instance_attribute(
            instance_id=instance.id,
            attribute='groupSet',
            value=flintrock_base_group)

    # TODO: Figure out if we want to use "node" instead of "instance" when
    #       communicating with the user, even if we're talking about doing things
    #       to EC2 instances. Spark docs definitely favor "node".
    print("Terminating {c} instances...".format(c=len(cluster_instances)))
    connection.terminate_instances(
        instance_ids=[instance.id for instance in cluster_instances])

    # TODO: Centralize logic to get cluster security group name from cluster name.
    connection.delete_security_group(name='flintrock-' + cluster_name)


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
