import shlex
import json
import time
import urllib.request

import paramiko
from collections import namedtuple

# Flintrock modules.
from ..ssh import generate_ssh_key_pair
from ..ssh import ssh_check_output

# TODO: Cache these files. (?) They are being read potentially tens or
#       hundreds of times. Maybe it doesn't matter because the files
#       are so small.
# NOTE: functools.lru_cache() doesn't work here because the mapping is
#       not hashable.
# TODO: Get rid of this. Just escape braces à la {{ and }}.
def get_formatted_template(path: str, mapping: dict) -> str:
    class TemplateDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'

    with open(path) as f:
        formatted = f.read().format_map(TemplateDict(**mapping))

    return formatted

SparkTemplateVariables = namedtuple(
    'SparkTemplateVariables', [
        'name',
        'master_host',
        'slave_hosts',
        'spark_scratch_dir',
        'spark_master_opts'
    ])

# TODO: Turn this into an implementation of an abstract FlintrockModule class. (?)
class Spark:
    def __init__(self, version):
        self.version = version

    def install(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster_provider):

        """
        Downloads and installs Spark on a given node.
        """
        # TODO: Allow users to specify the Spark "distribution".
        distribution = 'hadoop1'

        host = ssh_client.get_transport().getpeername()[0]
        print("[{h}] Installing Spark...".format(h=host))

        # Collect information that is needed to fill in the templates
        (master_instance, slave_instances) = cluster_provider.get_cluster_instances()
        template_vars = SparkTemplateVariables(
            name=cluster_provider.cluster_name,
            master_host=master_instance.public_dns_name,
            slave_hosts=[instance.public_dns_name for instance in slave_instances],
            spark_scratch_dir='/mnt/spark',
            spark_master_opts="")

        try:
            # TODO: Figure out how these non-template paths should work.
            ssh_check_output(
                ssh_client=ssh_client,
                command="""
                    set -e

                    echo {f} > /tmp/install-spark.sh
                    chmod 755 /tmp/install-spark.sh

                    /tmp/install-spark.sh {spark_version} {distribution}
                """.format(
                    f=shlex.quote(
                        get_formatted_template(
                            path='./install-spark.sh',
                            mapping=vars(template_vars))),
                    spark_version=shlex.quote(self.version),
                    distribution=shlex.quote(distribution)))
        except Exception as e:
            print("Could not find package for Spark {s} / {d}.".format(
                    s=self.version,
                    d=distribution
                ), file=sys.stderr)
            raise


    def configure(self,
                  ssh_client: paramiko.client.SSHClient,
                  cluster_provider):
        """
        Runs after all nodes are "ready".
        """

        host = ssh_client.get_transport().getpeername()[0]
        print("[{h}] Configuring Spark...".format(h=host))

        # Collect information that is needed to fill in the templates
        (master_instance, slave_instances) = cluster_provider.get_cluster_instances()
        template_vars = SparkTemplateVariables(
            name=cluster_provider.cluster_name,
            master_host=master_instance.public_dns_name,
            slave_hosts=[instance.public_dns_name for instance in slave_instances],
            spark_scratch_dir='/mnt/spark',
            spark_master_opts="")

        template_path = "./spark/conf/spark-env.sh"
        ssh_check_output(
            ssh_client=ssh_client,
            command="""
                echo {f} > {p}
            """.format(
                f=shlex.quote(
                    get_formatted_template(
                        path="templates/" + template_path,
                        mapping=vars(template_vars))),
                p=shlex.quote(template_path)))

    def configure_master(
            self,
            ssh_client: paramiko.client.SSHClient,
            cluster_provider):
        """
        Configures the Spark master and starts both the master and slaves.
        """
        host = ssh_client.get_transport().getpeername()[0]
        print("[{h}] Configuring Spark master...".format(h=host))

        (master_instance, slave_instances) = cluster_provider.get_cluster_instances()
        slave_hosts = [instance.public_dns_name for instance in slave_instances]
        master_host = master_instance.public_dns_name

        # TODO: Maybe move this shell script out to some separate file/folder
        #       for the Spark module.
        ssh_check_output(
            ssh_client=ssh_client,
            command="""
                set -e

                echo {s} > spark/conf/slaves

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
                s=shlex.quote('\n'.join(slave_hosts)),
                m=shlex.quote(master_host)))

        # Spark health check
        # TODO: Move to health_check() module method?
        # TODO: Research (or implement) way to get Spark to tell you when
        #       it's ready, as opposed to checking after a time delay.
        time.sleep(10)

        spark_master_ui = 'http://{m}:8080/json/'.format(m=master_host)

        spark_ui_info = json.loads(
            urllib.request.urlopen(spark_master_ui).read().decode('utf-8'))

        import textwrap
        print(textwrap.dedent(
            """\
            Spark Health Report:
              * Master: {status}
              * Workers: {workers}
              * Cores: {cores}
              * Memory: {memory:.1f} GB

            Web UI available at: http://{m}:8080
            """.format(
                status=spark_ui_info['status'],
                workers=len(spark_ui_info['workers']),
                cores=spark_ui_info['cores'],
                memory=spark_ui_info['memory'] / 1024,
                m = master_host)))

    def configure_slave(self):
        pass

