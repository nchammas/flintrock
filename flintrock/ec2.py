import functools
import string
import sys
import time
import urllib.request
import base64
import logging
from ipaddress import IPv4Network
from datetime import datetime

# External modules
import boto3
import botocore
import click

# Flintrock modules
from .core import FlintrockCluster
from .core import provision_cluster
from .exceptions import (
    Error,
    ClusterNotFound,
    ClusterAlreadyExists,
    ClusterInvalidState,
    InterruptedEC2Operation,
    NothingToDo,
)
from .ssh import generate_ssh_key_pair
from .services import SecurityGroupRule
from .util import duration_to_expiration

logger = logging.getLogger('flintrock.ec2')


class NoDefaultVPC(Error):
    def __init__(self, *, region: str):
        super().__init__(
            "Flintrock could not find a default VPC in {r}. "
            "Please explicitly specify a VPC to work with in that region. "
            "Flintrock does not support managing EC2 clusters outside a VPC."
            .format(r=region)
        )
        self.region = region


class ConfigurationNotSupported(Error):
    def __init__(self, message):
        super().__init__(message)


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now().replace(microsecond=0)
        res = func(*args, **kwargs)
        end = datetime.now().replace(microsecond=0)
        logger.info("{f} finished in {t}.".format(f=func.__name__, t=(end - start)))
        return res
    return wrapper


class EC2Cluster(FlintrockCluster):
    def __init__(
            self,
            region: str,
            vpc_id: str,
            master_instance: 'boto3.resources.factory.ec2.Instance',
            slave_instances: "List[boto3.resources.factory.ec2.Instance]",
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.region = region
        self.vpc_id = vpc_id
        self.master_instance = master_instance
        self.slave_instances = slave_instances

    @property
    def instances(self):
        if self.master_instance:
            return [self.master_instance] + self.slave_instances
        else:
            return self.slave_instances

    @property
    @functools.lru_cache()
    def private_network(self):
        ec2 = boto3.resource(service_name='ec2', region_name=self.region)
        return not ec2.Subnet(self.master_instance.subnet_id).map_public_ip_on_launch

    @property
    def master_ip(self):
        if self.private_network:
            return self.master_instance.private_ip_address
        else:
            return self.master_instance.public_ip_address

    @property
    def master_host(self):
        if self.private_network:
            return self.master_instance.private_dns_name
        else:
            return self.master_instance.public_dns_name

    @property
    def master_private_host(self):
        return self.master_instance.private_dns_name

    @property
    def slave_ips(self):
        if self.private_network:
            return [i.private_ip_address for i in self.slave_instances]
        else:
            return [i.public_ip_address for i in self.slave_instances]

    @property
    def slave_hosts(self):
        if self.private_network:
            return [i.private_dns_name for i in self.slave_instances]
        else:
            return [i.public_dns_name for i in self.slave_instances]

    @property
    def slave_private_hosts(self):
        return [i.private_dns_name for i in self.slave_instances]

    @property
    def num_masters(self):
        return 1 if self.master_instance else 0

    @property
    def num_slaves(self):
        return len(self.slave_instances)

    @property
    def state(self):
        instance_states = set(
            instance.state['Name'] for instance in self.instances)
        if len(instance_states) == 1:
            return instance_states.pop()
        else:
            return 'inconsistent'

    def wait_for_state(self, state: str):
        """
        Wait for the cluster's instances to a reach a specific state.
        The state of any services installed on the cluster is a
        separate matter.

        This method updates the cluster's instance metadata and
        master and slave IP addresses and hostnames.
        """
        ec2 = boto3.resource(service_name='ec2', region_name=self.region)

        while any([i.state['Name'] != state for i in self.instances]):
            if logger.isEnabledFor(logging.DEBUG):
                waiting_instances = [i for i in self.instances if i.state['Name'] != state]
                sample = ', '.join(["'{}'".format(i.id) for i in waiting_instances][:3])
                logger.debug("{size} instances not in state '{state}': {sample}, ...".format(size=len(waiting_instances), state=state, sample=sample))
            time.sleep(3)
            # Update metadata for all instances in one shot. We don't want
            # to make a call to AWS for each of potentially hundreds of
            # instances.
            instances = list(
                ec2.instances.filter(
                    # NOTE: We use Filters instead of InstanceIds to avoid
                    #       the issue described here: https://github.com/boto/boto3/issues/479
                    Filters=[
                        {'Name': 'instance-id', 'Values': [i.id for i in self.instances]}
                    ]))
            (self.master_instance, self.slave_instances) = _get_cluster_master_slaves(instances)

    def destroy(self):
        self.destroy_check()
        super().destroy()
        ec2 = boto3.resource(service_name='ec2', region_name=self.region)

        # TODO: Centralize logic to get Flintrock base security group. (?)
        flintrock_base_group = list(
            ec2.security_groups.filter(
                Filters=[
                    {'Name': 'group-name', 'Values': ['flintrock']},
                    {'Name': 'vpc-id', 'Values': [self.vpc_id]},
                ]))[0]

        # We "unassign" the cluster security group here (i.e. the
        # 'flintrock-clustername' group) so that we can immediately delete it once
        # the instances are terminated. If we don't do this, we get dependency
        # violations for a couple of minutes before we can actually delete the group.
        # TODO: Is there a way to do this in one call for all instances?
        #       Do we need to throttle these calls?
        for instance in self.instances:
            instance.modify_attribute(
                Groups=[flintrock_base_group.id])

        # TODO: Centralize logic to get cluster security group name from cluster name.
        cluster_group = list(
            ec2.security_groups.filter(
                Filters=[
                    {'Name': 'group-name', 'Values': ['flintrock-' + self.name]},
                    {'Name': 'vpc-id', 'Values': [self.vpc_id]},
                ]))[0]
        cluster_group.delete()

        (ec2.instances
            .filter(
                Filters=[
                    {'Name': 'instance-id', 'Values': [i.id for i in self.instances]}
                ])
            .terminate())
        self.wait_for_state('terminated')

    def start_check(self):
        if self.state == 'running':
            raise NothingToDo("Cluster is already running.")
        elif self.state != 'stopped':
            raise ClusterInvalidState(
                attempted_command='start',
                state=self.state)

    @timeit
    def start(self, *, user: str, identity_file: str):
        # TODO: Do these _check() methods make sense here?
        self.start_check()
        ec2 = boto3.resource(service_name='ec2', region_name=self.region)
        (ec2.instances
            .filter(
                Filters=[
                    {'Name': 'instance-id', 'Values': [i.id for i in self.instances]}
                ])
            .start())
        self.wait_for_state('running')

        super().start(
            user=user,
            identity_file=identity_file)

    def stop_check(self):
        if self.state == 'stopped':
            raise NothingToDo("Cluster is already stopped.")
        elif self.state != 'running':
            raise ClusterInvalidState(
                attempted_command='stop',
                state=self.state)

    @timeit
    def stop(self):
        self.stop_check()
        super().stop()

        ec2 = boto3.resource(service_name='ec2', region_name=self.region)
        (ec2.instances
            .filter(
                Filters=[
                    {'Name': 'instance-id', 'Values': [i.id for i in self.instances]}
                ])
            .stop())
        self.wait_for_state('stopped')

    def add_slaves_check(self):
        if self.state != 'running':
            raise ClusterInvalidState(
                attempted_command='add-slaves',
                state=self.state)

    @timeit
    def add_slaves(
            self,
            *,
            user: str,
            identity_file: str,
            num_slaves: int,
            spot_price: float,
            spot_request_duration: str,
            min_root_ebs_size_gb: int,
            tags: list,
            assume_yes: bool):
        security_group_ids = [
            group['GroupId']
            for group in self.master_instance.security_groups]
        block_device_mappings = get_ec2_block_device_mappings(
            min_root_ebs_size_gb=min_root_ebs_size_gb,
            ami=self.master_instance.image_id,
            region=self.region)
        availability_zone = self.master_instance.placement['AvailabilityZone']

        ec2 = boto3.resource(service_name='ec2', region_name=self.region)
        client = ec2.meta.client

        response = client.describe_instance_attribute(
            InstanceId=self.master_instance.id,
            Attribute='instanceInitiatedShutdownBehavior'
        )
        instance_initiated_shutdown_behavior = response['InstanceInitiatedShutdownBehavior']['Value']

        response = client.describe_instance_attribute(
            InstanceId=self.master_instance.id,
            Attribute='userData'
        )
        if not response['UserData']:
            user_data = ''
        else:
            user_data = (
                base64.b64decode(response['UserData']['Value'])
                .decode('utf-8')
            )

        if not self.master_instance.iam_instance_profile:
            instance_profile_arn = ''
        else:
            instance_profile_arn = self.master_instance.iam_instance_profile['Arn']

        self.add_slaves_check()
        try:
            new_slave_instances = _create_instances(
                num_instances=num_slaves,
                region=self.region,
                spot_price=spot_price,
                spot_request_valid_until=duration_to_expiration(spot_request_duration),
                ami=self.master_instance.image_id,
                assume_yes=assume_yes,
                key_name=self.master_instance.key_name,
                instance_type=self.master_instance.instance_type,
                block_device_mappings=block_device_mappings,
                availability_zone=availability_zone,
                placement_group=self.master_instance.placement['GroupName'],
                tenancy=self.master_instance.placement['Tenancy'],
                security_group_ids=security_group_ids,
                subnet_id=self.master_instance.subnet_id,
                instance_profile_arn=instance_profile_arn,
                ebs_optimized=self.master_instance.ebs_optimized,
                instance_initiated_shutdown_behavior=instance_initiated_shutdown_behavior,
                user_data=user_data)

            slave_tags = [
                {'Key': 'flintrock-role', 'Value': 'slave'},
                {'Key': 'Name', 'Value': '{c}-slave'.format(c=self.name)}]
            slave_tags += tags

            (ec2.instances
                .filter(
                    Filters=[
                        {'Name': 'instance-id', 'Values': [i.id for i in new_slave_instances]}
                    ])
                .create_tags(Tags=slave_tags))

            existing_slaves = self.slave_ips

            self.slave_instances += new_slave_instances
            self.wait_for_state('running')

            # We wait for the new instances to start running so they all have assigned
            # IP addresses.
            new_slaves = set(self.slave_ips) - set(existing_slaves)

            super().add_slaves(
                user=user,
                identity_file=identity_file,
                new_hosts=new_slaves,
            )
        except (Exception, KeyboardInterrupt) as e:
            if isinstance(e, InterruptedEC2Operation):
                cleanup_instances = e.instances
            else:
                cleanup_instances = new_slave_instances
            _cleanup_instances(
                instances=cleanup_instances,
                assume_yes=assume_yes,
                region=self.region,
            )
            raise

    @timeit
    def remove_slaves(self, *, user: str, identity_file: str, num_slaves: int):
        ec2 = boto3.resource(service_name='ec2', region_name=self.region)

        # self.remove_slaves_check() (?)

        # Remove spot instances first, if any.
        _instances = sorted(
            self.slave_instances,
            key=lambda x: x.instance_lifecycle == 'spot',
            reverse=True)
        removed_slave_instances, self.slave_instances = \
            _instances[0:num_slaves], _instances[num_slaves:]

        if self.state == 'running':
            super().remove_slaves(user=user, identity_file=identity_file)

        # TODO: Centralize logic to get Flintrock base security group.
        flintrock_base_group = list(
            ec2.security_groups.filter(
                Filters=[
                    {'Name': 'group-name', 'Values': ['flintrock']},
                    {'Name': 'vpc-id', 'Values': [self.vpc_id]},
                ]))[0]

        # TODO: Is there a way to do this in one call for all instances?
        for instance in removed_slave_instances:
            instance.modify_attribute(
                Groups=[flintrock_base_group.id])

        (ec2.instances
            .filter(
                Filters=[
                    {'Name': 'instance-id', 'Values': [i.id for i in removed_slave_instances]}
                ])
            .terminate())

    def run_command_check(self):
        if self.state != 'running':
            raise ClusterInvalidState(
                attempted_command='run-command',
                state=self.state)

    @timeit
    def run_command(self, *, master_only, command, user, identity_file):
        self.run_command_check()
        super().run_command(
            master_only=master_only,
            user=user,
            identity_file=identity_file,
            command=command)

    def copy_file_check(self):
        if self.state != 'running':
            raise ClusterInvalidState(
                attempted_command='copy-file',
                state=self.state)

    @timeit
    def copy_file(self, *, local_path, remote_path, master_only=False, user, identity_file):
        self.copy_file_check()
        super().copy_file(
            master_only=master_only,
            user=user,
            identity_file=identity_file,
            local_path=local_path,
            remote_path=remote_path)

    def print(self):
        """
        Print information about the cluster to screen in YAML.

        We don't use PyYAML because we want to control the key order
        in the output.
        """
        # Mark the boundaries of the YAML output.
        # See: http://yaml.org/spec/current.html#id2525905
        # print('---')
        print(self.name + ':')
        print('  state: {s}'.format(s=self.state))
        print('  node-count: {nc}'.format(nc=len(self.instances)))
        if self.state == 'running':
            print('  master:', self.master_host if self.num_masters > 0 else '')
            print(
                '\n    - '.join(
                    ['  slaves:'] + (self.slave_hosts if self.num_slaves > 0 else [])))
        # print('...')


def get_default_vpc(region: str) -> 'boto3.resources.factory.ec2.Vpc':
    """
    Get the user's default VPC in the provided region.
    """
    ec2 = boto3.resource(service_name='ec2', region_name=region)

    default_vpc = list(
        ec2.vpcs.filter(
            Filters=[{'Name': 'isDefault', 'Values': ['true']}]))

    if default_vpc:
        return default_vpc[0]
    else:
        raise NoDefaultVPC(region=region)


def check_network_config(*, region_name: str, vpc_id: str, subnet_id: str):
    """
    Check that the VPC and subnet are configured to allow Flintrock to create
    clusters.
    """
    ec2 = boto3.resource(service_name='ec2', region_name=region_name)

    if not ec2.Vpc(vpc_id).describe_attribute(Attribute='enableDnsHostnames')['EnableDnsHostnames']['Value']:
        raise ConfigurationNotSupported(
            "{v} does not have DNS hostnames enabled. "
            "Flintrock requires DNS hostnames to be enabled.\n"
            "See: https://github.com/nchammas/flintrock/issues/43"
            .format(v=vpc_id)
        )
    if not ec2.Subnet(subnet_id).map_public_ip_on_launch:
        logger.info(
            "{s} does not auto-assign public IP addresses. "
            "Flintrock will configure this cluster for private network access."
            .format(s=subnet_id)
        )


def get_security_groups(
        *,
        vpc_id,
        region,
        security_group_names) -> "List[boto3.resource('ec2').SecurityGroup]":
    ec2 = boto3.resource(service_name='ec2', region_name=region)

    groups = list(
        ec2.security_groups.filter(
            Filters=[
                {'Name': 'group-name', 'Values': security_group_names},
                {'Name': 'vpc-id', 'Values': [vpc_id]},
            ]))

    found_group_names = [group.group_name for group in groups]
    missing_group_names = set(security_group_names) - set(found_group_names)
    if missing_group_names:
        raise Error(
            "Could not find the following security group{s}: {groups}"
            .format(
                s='' if len(missing_group_names) == 1 else 's',
                groups=', '.join(list(missing_group_names))))

    return groups


def get_ssh_security_group_rules(
    *,
    flintrock_client_cidr=None,
    flintrock_client_group=None,
) -> "boto3.resource('ec2').SecurityGroup":
    return SecurityGroupRule(
        ip_protocol='tcp',
        from_port=22,
        to_port=22,
        cidr_ip=flintrock_client_cidr,
        src_group=flintrock_client_group,
    )


def get_or_create_flintrock_security_groups(
        *,
        cluster_name,
        vpc_id,
        region,
        services,
        ec2_authorize_access_from,
) -> "List[boto3.resource('ec2').SecurityGroup]":
    """
    If they do not already exist, create all the security groups needed for a
    Flintrock cluster.
    """
    ec2 = boto3.resource(service_name='ec2', region_name=region)

    # TODO: Make these into methods, since we need this logic (though simple)
    #       in multiple places. (?)
    flintrock_group_name = 'flintrock'
    cluster_group_name = 'flintrock-' + cluster_name

    # The Flintrock group is common to all Flintrock clusters and authorizes client traffic
    # to them.
    flintrock_group = list(
        ec2.security_groups.filter(
            Filters=[
                {'Name': 'group-name', 'Values': [flintrock_group_name]},
                {'Name': 'vpc-id', 'Values': [vpc_id]},
            ]))
    flintrock_group = flintrock_group[0] if flintrock_group else None

    # The cluster group is specific to one Flintrock cluster and authorizes intra-cluster
    # communication.
    cluster_group = list(
        ec2.security_groups.filter(
            Filters=[
                {'Name': 'group-name', 'Values': [cluster_group_name]},
                {'Name': 'vpc-id', 'Values': [vpc_id]},
            ]))
    cluster_group = cluster_group[0] if cluster_group else None

    if not flintrock_group:
        flintrock_group = ec2.create_security_group(
            GroupName=flintrock_group_name,
            Description="Flintrock base group",
            VpcId=vpc_id)

    # Rules for the client interacting with the cluster.
    if ec2_authorize_access_from:
        flintrock_client_sources = ec2_authorize_access_from
    else:
        flintrock_client_ip = (
            urllib.request.urlopen('https://checkip.amazonaws.com/')
            .read().decode('utf-8').strip()
        )
        flintrock_client_sources = [flintrock_client_ip]

    client_rules = []
    for client_source in flintrock_client_sources:
        # Security group for SSH is always required
        if client_source.startswith('sg-'):
            client_rules.append(
                get_ssh_security_group_rules(flintrock_client_group=client_source)
            )
        else:
            client_rules.append(
                get_ssh_security_group_rules(flintrock_client_cidr=str(IPv4Network(client_source)))
            )
        # Service-specific security group rules
        for service in services:
            if client_source.startswith('sg-'):
                client_rules += service.get_security_group_rules(
                    flintrock_client_group=client_source,
                )
            else:
                client_rules += service.get_security_group_rules(
                    flintrock_client_cidr=str(IPv4Network(client_source)),
                )

    # Rules for internal cluster communication.
    if not cluster_group:
        cluster_group = ec2.create_security_group(
            GroupName=cluster_group_name,
            Description="Flintrock cluster group",
            VpcId=vpc_id)

    # TODO: Don't try adding rules that already exist.
    # TODO: Add rules in one shot.
    for rule in client_rules:
        try:
            cluster_group.authorize_ingress(
                IpProtocol=rule.ip_protocol,
                FromPort=rule.from_port,
                ToPort=rule.to_port,
                CidrIp=rule.cidr_ip,
                SourceSecurityGroupName=rule.src_group,
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'InvalidPermission.Duplicate':
                raise Exception("Error adding rule: {r}".format(r=rule))

    try:
        cluster_group.authorize_ingress(
            IpPermissions=[
                {
                    'IpProtocol': '-1',  # -1 means all
                    'FromPort': -1,
                    'ToPort': -1,
                    'UserIdGroupPairs': [{'GroupId': cluster_group.id}]
                }])
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'InvalidPermission.Duplicate':
            raise Exception("Error authorizing cluster ingress to self.") from e

    return [flintrock_group, cluster_group]


def get_ec2_block_device_mappings(
        *,
        min_root_ebs_size_gb: int,
        ami: str,
        region: str) -> 'List[dict]':
    """
    Get the block device map we should assign to instances launched from a given AMI.

    This is how we configure storage on the instance.
    """
    ec2 = boto3.resource(service_name='ec2', region_name=region)
    block_device_mappings = []

    try:
        image = list(
            ec2.images.filter(
                Filters=[
                    {'Name': 'image-id', 'Values': [ami]}
                ]))[0]
    except IndexError as e:
        raise Error(
            "Error: Could not find {ami} in region {region}.".format(
                ami=ami,
                region=region))

    if image.root_device_type == 'ebs':
        root_device = [
            device for device in image.block_device_mappings
            if device['DeviceName'] == image.root_device_name][0]
        if root_device['Ebs']['VolumeSize'] < min_root_ebs_size_gb:
            root_device['Ebs'].update({
                # Max root volume size for instance store-backed AMIs is 10 GiB.
                # See: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/add-instance-store-volumes.html
                # Though, this code is probably incorrect for instance store-backed
                # instances anyway, since boto3 doesn't seem to let you set the size
                # of a root instance store volume.
                'VolumeSize': min_root_ebs_size_gb,
                # gp2 is general-purpose SSD
                'VolumeType': 'gp2'})
        del root_device['Ebs']['Encrypted']
        block_device_mappings.append(root_device)

    for i in range(12):
        ephemeral_device = {
            'VirtualName': 'ephemeral' + str(i),
            'DeviceName': '/dev/sd' + string.ascii_lowercase[i + 1]}
        block_device_mappings.append(ephemeral_device)

    return block_device_mappings


def _create_instances(
        *,
        num_instances,
        region,
        spot_price,
        spot_request_valid_until,
        ami,
        assume_yes,
        key_name,
        instance_type,
        block_device_mappings,
        availability_zone,
        placement_group,
        tenancy,
        security_group_ids,
        subnet_id,
        instance_profile_arn,
        ebs_optimized,
        instance_initiated_shutdown_behavior,
        user_data) -> 'List[boto3.resources.factory.ec2.Instance]':
    ec2 = boto3.resource(service_name='ec2', region_name=region)

    cluster_instances = []
    spot_requests = []

    try:
        if spot_price:
            user_data = base64.b64encode(user_data.encode('utf-8')).decode()
            logger.info("Requesting {c} spot instances at a max price of ${p}...".format(
                c=num_instances, p=spot_price))
            client = ec2.meta.client
            spot_requests = client.request_spot_instances(
                SpotPrice=str(spot_price),
                InstanceCount=num_instances,
                ValidUntil=spot_request_valid_until,
                LaunchSpecification={
                    'ImageId': ami,
                    'KeyName': key_name,
                    'InstanceType': instance_type,
                    'BlockDeviceMappings': block_device_mappings,
                    'Placement': {
                        'AvailabilityZone': availability_zone,
                        'GroupName': placement_group},
                    'SecurityGroupIds': security_group_ids,
                    'SubnetId': subnet_id,
                    'IamInstanceProfile': {
                        'Arn': instance_profile_arn},
                    'EbsOptimized': ebs_optimized,
                    'UserData': user_data})['SpotInstanceRequests']

            request_ids = [r['SpotInstanceRequestId'] for r in spot_requests]
            pending_request_ids = request_ids

            while pending_request_ids:
                logger.info("{grant} of {req} instances granted. Waiting...".format(
                    grant=num_instances - len(pending_request_ids),
                    req=num_instances))
                time.sleep(30)
                spot_requests = client.describe_spot_instance_requests(
                    SpotInstanceRequestIds=request_ids)['SpotInstanceRequests']

                failed_requests = [r for r in spot_requests if r['State'] == 'failed']
                if failed_requests:
                    failure_reasons = {r['Status']['Code'] for r in failed_requests}
                    raise Error(
                        "The spot request failed for the following reason{s}: {reasons}"
                        .format(
                            s='' if len(failure_reasons) == 1 else 's',
                            reasons=', '.join(failure_reasons)))

                pending_request_ids = [
                    r['SpotInstanceRequestId'] for r in spot_requests
                    if r['State'] == 'open']

            logger.info("All {c} instances granted.".format(c=num_instances))

            cluster_instances = list(
                ec2.instances.filter(
                    Filters=[
                        {'Name': 'instance-id', 'Values': [r['InstanceId'] for r in spot_requests]}
                    ]))
        else:
            # Move this to flintrock.py?
            logger.info("Launching {c} instance{s}...".format(
                c=num_instances,
                s='' if num_instances == 1 else 's'))

            # TODO: If an exception is raised in here, some instances may be
            #       left stranded.
            cluster_instances = ec2.create_instances(
                MinCount=num_instances,
                MaxCount=num_instances,
                ImageId=ami,
                KeyName=key_name,
                InstanceType=instance_type,
                BlockDeviceMappings=block_device_mappings,
                Placement={
                    'AvailabilityZone': availability_zone,
                    'Tenancy': tenancy,
                    'GroupName': placement_group},
                SecurityGroupIds=security_group_ids,
                SubnetId=subnet_id,
                IamInstanceProfile={
                    'Arn': instance_profile_arn},
                EbsOptimized=ebs_optimized,
                InstanceInitiatedShutdownBehavior=instance_initiated_shutdown_behavior,
                UserData=user_data)
        time.sleep(10)  # AWS metadata eventual consistency tax.
        return cluster_instances
    except (Exception, KeyboardInterrupt) as e:
        if not isinstance(e, KeyboardInterrupt):
            print(e, file=sys.stderr)
        if spot_requests:
            request_ids = [r['SpotInstanceRequestId'] for r in spot_requests]
            if any([r['State'] != 'active' for r in spot_requests]):
                print("Canceling spot instance requests...", file=sys.stderr)
                client.cancel_spot_instance_requests(
                    SpotInstanceRequestIds=request_ids)
            # Make sure we have the latest information on any launched spot instances.
            spot_requests = client.describe_spot_instance_requests(
                SpotInstanceRequestIds=request_ids)['SpotInstanceRequests']
            instance_ids = [
                r['InstanceId'] for r in spot_requests
                if 'InstanceId' in r]
            if instance_ids:
                cluster_instances = list(
                    ec2.instances.filter(
                        Filters=[
                            {'Name': 'instance-id', 'Values': instance_ids}
                        ]))
        raise InterruptedEC2Operation(instances=cluster_instances) from e


@timeit
def launch(
        *,
        cluster_name,
        num_slaves,
        java_version,
        services,
        assume_yes,
        key_name,
        identity_file,
        instance_type,
        region,
        availability_zone,
        ami,
        user,
        security_groups,
        spot_price=None,
        spot_request_duration=None,
        min_root_ebs_size_gb,
        vpc_id,
        subnet_id,
        instance_profile_name,
        placement_group,
        tenancy='default',
        ebs_optimized=False,
        instance_initiated_shutdown_behavior='stop',
        user_data,
        tags,
        ec2_authorize_access_from):
    """
    Launch a cluster.
    """
    if not vpc_id:
        vpc_id = get_default_vpc(region=region).id
    else:
        # If it's a non-default VPC -- i.e. the user set it up -- make sure it's
        # configured correctly.
        check_network_config(
            region_name=region,
            vpc_id=vpc_id,
            subnet_id=subnet_id)

    try:
        get_cluster(
            cluster_name=cluster_name,
            region=region,
            vpc_id=vpc_id)
    except ClusterNotFound as e:
        pass
    else:
        raise ClusterAlreadyExists(
            "Cluster {c} already exists in region {r}, VPC {v}.".format(
                c=cluster_name,
                r=region,
                v=vpc_id))

    flintrock_security_groups = get_or_create_flintrock_security_groups(
        cluster_name=cluster_name,
        vpc_id=vpc_id,
        region=region,
        services=services,
        ec2_authorize_access_from=ec2_authorize_access_from)
    user_security_groups = get_security_groups(
        vpc_id=vpc_id,
        region=region,
        security_group_names=security_groups)
    security_group_ids = [sg.id for sg in user_security_groups + flintrock_security_groups]
    block_device_mappings = get_ec2_block_device_mappings(
        min_root_ebs_size_gb=min_root_ebs_size_gb,
        ami=ami,
        region=region)

    ec2 = boto3.resource(service_name='ec2', region_name=region)
    iam = boto3.resource(service_name='iam', region_name=region)

    # We use IAM profile ARNs internally because AWS's API prefers that in
    # a few places.
    # See: https://github.com/boto/boto3/issues/769
    if instance_profile_name:
        instance_profile_arn = iam.InstanceProfile(instance_profile_name).arn
    else:
        instance_profile_arn = ''

    num_instances = num_slaves + 1
    if user_data is not None:
        user_data = user_data.read()
    else:
        user_data = ''

    try:
        cluster_instances = _create_instances(
            num_instances=num_instances,
            region=region,
            spot_price=spot_price,
            spot_request_valid_until=duration_to_expiration(spot_request_duration),
            ami=ami,
            assume_yes=assume_yes,
            key_name=key_name,
            instance_type=instance_type,
            block_device_mappings=block_device_mappings,
            availability_zone=availability_zone,
            placement_group=placement_group,
            tenancy=tenancy,
            security_group_ids=security_group_ids,
            subnet_id=subnet_id,
            instance_profile_arn=instance_profile_arn,
            ebs_optimized=ebs_optimized,
            instance_initiated_shutdown_behavior=instance_initiated_shutdown_behavior,
            user_data=user_data)

        master_instance = cluster_instances[0]
        slave_instances = cluster_instances[1:]

        master_tags = [
            {'Key': 'flintrock-role', 'Value': 'master'},
            {'Key': 'Name', 'Value': '{c}-master'.format(c=cluster_name)}]
        master_tags += tags

        (ec2.instances
            .filter(
                Filters=[
                    {'Name': 'instance-id', 'Values': [master_instance.id]}
                ])
            .create_tags(Tags=master_tags))

        slave_tags = [
            {'Key': 'flintrock-role', 'Value': 'slave'},
            {'Key': 'Name', 'Value': '{c}-slave'.format(c=cluster_name)}]
        slave_tags += tags

        (ec2.instances
            .filter(
                Filters=[
                    {'Name': 'instance-id', 'Values': [i.id for i in slave_instances]}
                ])
            .create_tags(Tags=slave_tags))

        cluster = EC2Cluster(
            name=cluster_name,
            region=region,
            vpc_id=vpc_id,
            ssh_key_pair=generate_ssh_key_pair(),
            master_instance=master_instance,
            slave_instances=slave_instances)

        cluster.wait_for_state('running')

        provision_cluster(
            cluster=cluster,
            java_version=java_version,
            services=services,
            user=user,
            identity_file=identity_file)

        return cluster
    except (Exception, KeyboardInterrupt) as e:
        if isinstance(e, InterruptedEC2Operation):
            cleanup_instances = e.instances
        else:
            # TODO: There is no guarantee that cluster_instances is
            #       defined.
            # See: https://github.com/nchammas/flintrock/issues/183
            cleanup_instances = cluster_instances
        _cleanup_instances(
            instances=cleanup_instances,
            assume_yes=assume_yes,
            region=region,
        )
        raise


def get_cluster(*, cluster_name: str, region: str, vpc_id: str) -> EC2Cluster:
    """
    Get an existing EC2 cluster.
    """
    cluster = get_clusters(
        cluster_names=[cluster_name],
        region=region,
        vpc_id=vpc_id)
    return cluster[0]


def get_clusters(*, cluster_names: list=[], region: str, vpc_id: str) -> list:
    """
    Get all the named clusters. If no names are given, get all clusters.

    We do a little extra work here so that we only make one call to AWS
    regardless of how many clusters we have to look up. That's because querying
    AWS -- a network operation -- is by far the slowest step.
    """
    ec2 = boto3.resource(service_name='ec2', region_name=region)
    if not vpc_id:
        vpc_id = get_default_vpc(region=region).id

    if cluster_names:
        group_name_filter = ['flintrock-' + cn for cn in cluster_names]
    else:
        group_name_filter = ['flintrock']

    all_clusters_instances = list(
        ec2.instances.filter(
            Filters=[
                {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped']},
                {'Name': 'instance.group-name', 'Values': group_name_filter},
                {'Name': 'vpc-id', 'Values': [vpc_id]},
            ]))

    found_cluster_names = {
        _get_cluster_name(instance) for instance in all_clusters_instances}

    if cluster_names:
        missing_cluster_names = set(cluster_names) - found_cluster_names
        if missing_cluster_names:
            raise ClusterNotFound("No cluster {c} in region {r}.".format(
                c=missing_cluster_names.pop(),
                r=region))

    clusters = [
        _compose_cluster(
            name=cluster_name,
            region=region,
            vpc_id=vpc_id,
            instances=list(filter(
                lambda x: _get_cluster_name(x) == cluster_name, all_clusters_instances)))
        for cluster_name in found_cluster_names]

    return clusters


def cli_validate_tags(ctx, param, value):
    return validate_tags(value)


def validate_tags(value):
    """
    Validate and parse optional EC2 tags.
    """
    err_msg = ("Tags need to be specified as 'Key,Value' pairs "
               "separated by a single comma. Key cannot be empty "
               "or be made up entirely of whitespace.")
    tags = value
    result = []
    for tag in tags:
        if tag.count(',') != 1:
            raise click.BadParameter(err_msg)
        key, value = [word.strip() for word in tag.split(',', maxsplit=1)]
        if not key:
            raise click.BadParameter(err_msg)
        result.append({'Key': key, 'Value': value})

    return result


def cli_validate_ec2_authorize_access(ctx, param, value):
    return validate_ec2_authorize_access(value)


def validate_ec2_authorize_access(value):
    """
    Validate and parse optional EC2 security groups or CIDRs
    authorized to connect to cluster.
    """
    validated_addresses = []
    for address in value:
        if address.startswith('sg-'):
            validated_addresses.append(address)
            continue
        else:
            try:
                ipv4_network = IPv4Network(address)
                validated_addresses.append(str(ipv4_network))
            except ValueError:
                raise click.BadParameter(
                    "'{}' appears to be neither an IP address nor a Security Group ID."
                    .format(address)
                )
    return validated_addresses


def _get_cluster_name(instance: 'boto3.resources.factory.ec2.Instance') -> str:
    """
    Given an EC2 instance, get the name of the Flintrock cluster it belongs to.
    """
    for group in instance.security_groups:
        if group['GroupName'].startswith('flintrock-'):
            return group['GroupName'].replace('flintrock-', '', 1)
    else:
        raise Exception("Could not extract cluster name from instance: {i}".format(
            i=instance.id))


def _get_cluster_master_slaves(
        instances: list) -> ('boto3.resources.factory.ec2.Instance', list):
    """
    Get the master and slave instances from a set of raw EC2 instances representing
    a Flintrock cluster.
    """
    master_instance = None
    slave_instances = []

    for instance in instances:
        if not instance.tags:
            # TODO: Better handle malformed clusters with missing tags.
            # See: https://github.com/nchammas/flintrock/issues/183
            continue
        for tag in instance.tags:
            if tag['Key'] == 'flintrock-role':
                if tag['Value'] == 'master':
                    if master_instance is not None:
                        raise Exception("More than one master found.")
                    else:
                        master_instance = instance
                        break
                elif tag['Value'] == 'slave':
                    slave_instances.append(instance)

    # if not master_instance:
    #     print("Warning: No master found.", file=sys.stderr)
    # elif not slave_instances:
    #     print("Warning: No slaves found.", file=sys.stderr)

    return (master_instance, slave_instances)


def _compose_cluster(*, name: str, region: str, vpc_id: str, instances: list) -> EC2Cluster:
    """
    Compose an EC2Cluster object from a set of raw EC2 instances representing
    a Flintrock cluster.
    """
    (master_instance, slave_instances) = _get_cluster_master_slaves(instances)

    cluster = EC2Cluster(
        name=name,
        region=region,
        vpc_id=vpc_id,
        master_instance=master_instance,
        slave_instances=slave_instances)

    return cluster


def _cleanup_instances(*, instances: list, assume_yes: bool, region: str):
    ec2 = boto3.resource(service_name='ec2', region_name=region)
    if instances:
        if not assume_yes:
            yes = click.confirm(
                text="Do you want to terminate the {c} instances created by this operation?"
                     .format(c=len(instances)),
                err=True,
                default=True)

        if assume_yes or yes:
            print("Terminating instances...", file=sys.stderr)
            (ec2.instances
                .filter(
                    Filters=[
                        {'Name': 'instance-id', 'Values': [i.id for i in instances]}
                    ])
                .terminate())
