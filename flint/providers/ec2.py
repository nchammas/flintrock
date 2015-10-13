
import sys
import time
import shlex
import functools
from collections import namedtuple

import click
import boto
import boto.ec2
import urllib.request

# Flintrock modules.
from ..util import timeit
from ..ssh import generate_ssh_key_pair
from ..ssh import ssh_open
from ..ssh import ssh_check_output
from ..ssh import ssh_login
from ..async import async_execute

# ---------------------------------------------------------------------------------
#                            ASYNC HELPER FUNCTIONS
# ---------------------------------------------------------------------------------

def async_provision_node(instance, ssh_key_pair, identity_file):
    """
    Connect to a freshly launched EC2 instance, set it up for SSH access.

    This function is intended to be called on all cluster nodes in parallel.

    No master- or slave-specific logic should be in this method.
    """
    with ssh_open(instance, "ec2-user", identity_file) as client:
        host = client.get_transport().getpeername()[0]
        print("[{h}] Provisioning and setting up fresh ssh keys...".format(h=host))

        # --- SSH is now available. Set up SSH keys. ---
        ssh_check_output(ssh_client = client,
            command = """
                set -e

                echo {private_key} > ~/.ssh/id_rsa
                echo {public_key} >> ~/.ssh/authorized_keys
                echo {public_key} > ~/.ssh/id_rsa.pub

                chmod 600 ~/.ssh/id_rsa
            """.format(
                private_key=shlex.quote(ssh_key_pair.private),
                public_key=shlex.quote(ssh_key_pair.public)))


def async_install_modules(cluster_provider, instance, identity_file):

    """
    Call the install() method for all modules. This helper function is intended
    to be called on all cluster nodes in parallel.

    No master- or slave-specific logic should be in the install() method.
    """
    with ssh_open(instance, user="ec2-user", identity_file=identity_file) as client:
        for module in cluster_provider.modules:
            module.install(ssh_client=client, cluster_provider=cluster_provider)


def async_configure_modules(cluster_provider, instance, identity_file):

    """
    Call the configure() method for all modules. This helper function is intended
    to be called on all cluster nodes in parallel.

    No master- or slave-specific logic should be in the configure() method.
    """
    with ssh_open(instance, user="ec2-user", identity_file=identity_file) as client:
        for module in cluster_provider.modules:
            module.configure(ssh_client=client, cluster_provider=cluster_provider)


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


# ---------------------------------------------------------------------------------
#                            AmazonEc2Provider Methods
# ---------------------------------------------------------------------------------

class AmazonEc2Provider(object):

    def __init__(self, cluster_name, modules, region):


        # Store the EC2 region
        self.cluster_name = cluster_name
        self.modules = modules
        self.region = region

    def get_master_instance(self):
        connection = boto.ec2.connect_to_region(region_name=self.region)

        try:
            cluster_instances = connection.get_only_instances(
                filters={
                    'instance.group-name': 'flintrock-' + self.cluster_name,
                    'tag:flintrock-role': 'master',
                })

            # Filter out terminated instances, since these hang around for a while
            # and trip up the cluster discovery logic.
            cluster_instances = [ instance for instance in cluster_instances
                                  if (instance.state != "terminated" and
                                      instance.state != "shutting-down") ]

            if len(cluster_instances) == 1:
                return cluster_instances[0]
            else:
                print("Error: found multiple master nodes.")
                sys.exit(1)

        except boto.exception.EC2ResponseError as e:
            print("An error occurred when querying EC2 for instance information.")
            sys.exit(0)


    def get_slave_instances(self):
        connection = boto.ec2.connect_to_region(region_name=self.region)

        try:
            cluster_instances = connection.get_only_instances(
                filters={
                    'instance.group-name': 'flintrock-' + self.cluster_name,
                    'tag:flintrock-role': 'slave',
                })

            # Filter out terminated instances, since these hang around for a while
            # and trip up the cluster discovery logic.
            cluster_instances = [ instance for instance in cluster_instances
                                  if (instance.state != "terminated" and
                                      instance.state != "shutting-down") ]

            return cluster_instances

        except boto.exception.EC2ResponseError as e:
            print("An error occurred when querying EC2 for instance information.")
            sys.exit(0)

    def get_cluster_instances(self):
        """
        Returns a tuple: (master_instance, slave_instances) with the latter being a list of instances
        """
        return (self.get_master_instance(), self.get_slave_instances())


    @timeit
    def launch_cluster(self, *,
               num_slaves,
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

        Parameters
        ----------

        num_slaves: int
          Number of slave nodes to start.

        key_name: str
          The name of the Amazon key to use when creating the cluster.  This key is installed by default on
          newly started nodes.

        identity_file: str
          The local copy of the SSH keys corresponding to key_name above.

        instance_type: str
          The type of instance to start (see https://aws.amazon.com/ec2/instance-types/)

        region: str
          An amazon region string, specifying which region where nodes should be requested.

        availability_zone: str
          Availability zone for newly started nodes.

        ami: str
          The Amazon machine image specifier.

        spot_price: float
          The bidding price for spot instances (slave nodes only). Set to None to disable spot instance requsets.

        vpc_id: str
          Specify an Amazon Virtual Private Cloud (VPC) network name to join.

        subnet_id: str
          Specify a VPC subnet to join.

        placement_group: str
          Specify an EC2 placement group.

        tenancy: str

        ebs_optimized: bool

        instance_initiated_shutdown_behavior: str
        """
        connection = boto.ec2.connect_to_region(region_name=region)
        print("Launching new cluster {h} with 1 master and {s} slave instances...".format(h=self.cluster_name,s=num_slaves))

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

        security_groups = get_or_create_security_groups(cluster_name=self.cluster_name, vpc_id=vpc_id)

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
                    instance.update()

                    if instance.state == 'running':
                        continue
                    else:
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
                    'Name': '{c}-master'.format(c=self.cluster_name)})
            connection.create_tags(
                resource_ids=[i.id for i in slave_instances],
                tags={
                    'flintrock-role': 'slave',
                    'Name': '{c}-slave'.format(c=self.cluster_name)})

            # Fetch the latest instance metadata before we set up the cluster.
            master_instance, slave_instances = self.get_cluster_instances()
            print(master_instance.public_dns_name, [inst.public_dns_name for inst in slave_instances])
            instances = [master_instance] + slave_instances

            # Provision nodes and set up SSH keys (in parallel)
            print("Waiting for cluster '{c}' to be SSH ready.".format(c=self.cluster_name))
            ssh_key_pair = generate_ssh_key_pair()
            tasks = [ functools.partial(async_provision_node,
                                        instance = instance,
                                        ssh_key_pair = ssh_key_pair,
                                        identity_file = identity_file) for instance in instances ]
            async_execute(tasks)

            # Install modules (in parallel)
            tasks = [ functools.partial(async_install_modules,
                                        cluster_provider = self,
                                        instance = instance,
                                        identity_file = identity_file) for instance in instances ]
            async_execute(tasks)

            # Configure modules (in parallel)
            tasks = [ functools.partial(async_configure_modules,
                                        cluster_provider = self,
                                        instance = instance,
                                        identity_file = identity_file) for instance in instances ]
            async_execute(tasks)

            # Finally, do any master-specific module configuration
            with ssh_open(master_instance, user="ec2-user", identity_file=identity_file) as client:
                for module in self.modules:
                    module.configure_master(ssh_client=client, cluster_provider=self)

            print("All {c} instances provisioned.  Modules are installed and configured.".format(
                c=len(instances)))


        except KeyboardInterrupt as e:
            print("Exiting...")
            sys.exit(1)
        # finally:
        #     print("Terminating all {c} instances...".format(
        #         c=len(reservation.instances)))

        #     for instance in reservation.instances:
        #         instance.terminate()



    #
    def destroy_cluster(self, *, assume_yes=True, region):
        """
        Destroy a cluster.  The cluster can be running or stopped.

        Parameters
        ----------

        assume_yes: bool
          Set to True to destroy the cluster without interactively confirming with the user first.
          The assume_yes parameter defaults to True here for library use (as opposed to command-line use).

        region: str
          An Amazon EC2 region name.
        """

        (master_node, slave_nodes) = self.get_cluster_instances()
        instances = [master_node] + slave_nodes

        # Should this be an error? ClusterNotFound exception?
        if not instances:
            print("No such cluster.")
            sys.exit(0)
            # Style: Should everything else be under an else: block?

        if not assume_yes:
            print_cluster_info(
                cluster_name=self.cluster_name,
                cluster_instances=instances)

            print('---')

            click.confirm(
                text="Are you sure you want to destroy this cluster?",
                abort=True)

        # TODO: Figure out if we want to use "node" instead of "instance" when
        #       communicating with the user, even if we're talking about doing things
        #       to EC2 instances. Spark docs definitely favor "node".
        print("Terminating {c} instances...".format(c=len(instances)))
        for instance in instances:
            instance.terminate()

        # TODO: Destroy cluster security group. We're not reusing it.

    def add_slaves(self, num_slaves, identity_file):
        pass

    def remove_slaves(self, num_slaves, assume_yes=True):
        pass


    def login(self, region, identity_file, ssh_tunnel_ports):
        """
        Log into the master node on a running cluster.

        Parameters
        ----------

        identity_file: str
          Path to a set of Amazon IAM SSH keys that can be used to log into and control the cluster.

        ssh_tunnel_ports: str
          Set up ssh port forwarding when you login to the cluster.
          This provides a convenient alternative to connecting to iPython
          notebook over an open port using SSL.  You must supply an argument
          of the form "local_port:remote_port"
        """

        master_instance = self.get_master_instance()
        if not master_instance:
            print("Cluster '{c}' does not exist.".format(c = self.cluster_name))
            sys.exit(0)

        if master_instance.state != "running":
            print("Could not log into cluster '{c}'.  Master node exists, but is not running.".format(c=self.cluster_name))
            sys.exit(0)

        ssh_login(user = "ec2-user",
                  host=master_instance.public_dns_name,
                  identity_file=identity_file,
                  ssh_tunnel_ports = ssh_tunnel_ports)

    def start_cluster(self, identity_file):
        """
        Re-start a stopped EC2 cluster.

        Parameters
        ----------

        identity_file: str
          Path to a set of Amazon IAM SSH keys that can be used to log into and control the cluster.
        """

        # Check: are the instances already running?
        (master_instance, slave_instances) = self.get_cluster_instances()
        instances = [master_instance] + slave_instances

        # Other error: report that cluster was not found.
        if not master_instance and not slave_instances:
            print("Cluster '{c}' does not exist.".format(c = self.cluster_name))
            sys.exit(0)

        # If the cluster is still stopping, report this to the user and recommend they wait to retry.
        if any([instance.state == "stopping" for instance in instances]):
            print("Cluster '{c}' is still stopping.  It must be fully stopped before you restart.".format(c = self.cluster_name))
            sys.exit(0)

        # If the cluster is stopped, start it.
        if all([instance.state == "stopped" for instance in instances]):
            print("Starting {c} instances...".format(c=len(instances)))
            for instance in instances:
                instance.start()

        # If we have made it this far, assume the cluster is in an ok state to
        # start up. Attempt to configure it. This will pause until ssh
        # connection become available.
        print("Waiting for cluster '{c}' to be SSH ready.".format(c=self.cluster_name))
        ssh_key_pair = generate_ssh_key_pair()

        # Provision nodes and set up SSH keys (in parallel). This also waits for
        # SSH to become available on the cluster.
        tasks = [ functools.partial(async_provision_node,
                                    instance = instance,
                                    ssh_key_pair = ssh_key_pair,
                                    identity_file = identity_file) for instance in instances ]
        async_execute(tasks)

        # Configure modules (in parallel)
        tasks = [ functools.partial(async_configure_modules,
                                    cluster_provider = self,
                                    instance = instance,
                                    identity_file = identity_file) for instance in instances ]
        async_execute(tasks)

        # Finally, do any master-specific module configuration
        with ssh_open(master_instance, user="ec2-user", identity_file=identity_file) as client:
            for module in self.modules:
                module.configure_master(ssh_client=client, cluster_provider=self)

        print("All {c} instances provisioned.  Modules are installed and configured.".format(
            c=len(instances)))


    def stop_cluster(self, assume_yes=True, wait_for_confirmation = False):
        """
        Stop a running EC2 cluster.

        Parameters
        ----------

        assume_yes : bool
          Stop the cluster without asking the user to confirm interactively.
          The assume_yes parameter defaults to True here for library use (as opposed to command-line use).

        wait_for_confirmation : bool
          Set to True to cause Flintrock to wait to exit until the cluster nodes are fully stopped.
          This can sometimes take a very long time, and is not usually necessary.
        """

        # Check: are the instances already running?
        (master_instance, slave_instances) = self.get_cluster_instances()
        instances = [master_instance] + slave_instances

        # Other error: report that cluster was not found.
        if not master_instance and not slave_instances:
            print("Cluster '{c}' does not exist.".format(c = self.cluster_name))
            sys.exit(0)

        # If the cluster is still stopping, report this to the user and recommend they wait to retry.
        if (any([instance.state == "stopping" for instance in instances]) or
            any([instance.state == "stopped" for instance in instances])):
            print("Cluster '{c}' is already stopping or has stopped.".format(c = self.cluster_name))
            sys.exit(0)

        # If we have made it this far, assume the cluster is in an ok state to
        # be stopped.

        if not assume_yes:
            print_cluster_info(
                cluster_name=self.cluster_name,
                cluster_instances=instances)

            print('---')

            click.confirm(
                text="Are you sure you want to stop this cluster?",
                abort=True)

        print("Stopping {c} instances...".format(c=len(instances)))
        for instance in instances:
            instance.stop()


        # Fully stopping an instance can sometimes take a very long time, and the user rarely needs
        # confirmation that the instance has stopped.  Only wait for a "stopped" status if the user
        # explicitly requests it.
        if wait_for_confirmation:
            desired_state = 'stopped'
        else:
            desired_state = 'stopping'


        while True:
            instance.update()
            for instance in instances:
                if instance.state == desired_state:
                    continue
                else:
                    time.sleep(3)
                    break
            else:
                print("Cluster '{c}' is now {ds}.".format(c=self.cluster_name, ds=desired_state))
                break

    def describe_cluster(self, *, master_hostname_only=False, region):
        connection = boto.ec2.connect_to_region(region_name=region)

        cluster_instances = connection.get_only_instances(
            filters={
                'instance.group-name': 'flintrock-' + self.cluster_name if self.cluster_name else 'flintrock',
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
                    if ('flintrock-' + self.cluster_name) in {g.name for g in instance.groups}:
                        filtered_instances.append(instance)

                print_cluster_info(
                    cluster_name=self.cluster_name,
                    cluster_instances=filtered_instances)
