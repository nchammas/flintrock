import os
import subprocess
import tempfile
import uuid
from collections import OrderedDict

# Flintrock
from flintrock.core import StorageDirs

# External
import pytest

HADOOP_VERSION = '2.8.5'
SPARK_VERSION = '2.4.0'
SPARK_GIT_COMMIT = '584354eaac02531c9584188b143367ba694b0c34'  # 2.0.2


class Dummy():
    pass


aws_credentials_required = (
    pytest.mark.skipif(
        not bool(os.environ.get('USE_AWS_CREDENTIALS')),
        reason="USE_AWS_CREDENTIALS not set"))


@pytest.fixture(scope='session')
def project_root_dir():
    return os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__)
        )
    )


@pytest.fixture(scope='session')
def dummy_cluster():
    storage_dirs = StorageDirs(
        root='/media/root',
        ephemeral=['/media/eph1', '/media/eph2'],
        persistent=None,
    )

    cluster = Dummy()
    cluster.name = 'test'
    cluster.storage_dirs = storage_dirs
    cluster.master_ip = '10.0.0.1'
    cluster.master_host = 'master.hostname'
    cluster.slave_ips = ['10.0.0.2']
    cluster.slave_hosts = ['slave1.hostname']

    return cluster


def random_string():
    return str(uuid.uuid4())[:8]


def launch_cluster(
        *,
        cluster_name,
        instance_type,
        spark_version,
        spark_git_commit):
    p = subprocess.run([
        'flintrock', 'launch', cluster_name,
        '--num-slaves', '1',
        '--install-hdfs',
        '--hdfs-version', HADOOP_VERSION,
        '--install-spark',
        '--spark-version', spark_version,
        '--spark-git-commit', spark_git_commit,
        '--assume-yes',
        '--ec2-instance-type', instance_type])
    assert p.returncode == 0


def stop_cluster(cluster_name):
    p = subprocess.run([
        'flintrock', 'stop', cluster_name, '--assume-yes'])
    assert p.returncode == 0


def start_cluster(cluster_name):
    p = subprocess.run([
        'flintrock', 'start', cluster_name])
    assert p.returncode == 0


# TODO: This should reuse FlintrockCluster.
class ClusterConfig:
    def __init__(
            self,
            *,
            restarted,
            instance_type,
            spark_version=SPARK_VERSION,
            spark_git_commit=''):
        self.restarted = restarted
        self.instance_type = instance_type
        self.spark_version = spark_version
        self.spark_git_commit = spark_git_commit

    def __str__(self):
        return str(OrderedDict(sorted(vars(self).items())))


cluster_configs = [
    ClusterConfig(restarted=False, instance_type='t2.small'),
    ClusterConfig(restarted=True, instance_type='t2.small'),
    ClusterConfig(restarted=False, instance_type='m3.medium'),
    ClusterConfig(restarted=True, instance_type='m3.medium'),
    # We don't test all cluster states when building Spark because
    # it takes a very long time.
    ClusterConfig(
        restarted=True,
        instance_type='m3.xlarge',
        spark_version='',
        spark_git_commit=SPARK_GIT_COMMIT)]


@pytest.fixture(
    scope='module',
    params=cluster_configs,
    ids=[str(cc) for cc in cluster_configs])
def running_cluster(request):
    """
    Return the name of a running Flintrock cluster.
    """
    cluster_name = 'running-cluster-' + random_string()
    launch_cluster(
        cluster_name=cluster_name,
        instance_type=request.param.instance_type,
        spark_version=request.param.spark_version,
        spark_git_commit=request.param.spark_git_commit)

    if request.param.restarted:
        stop_cluster(cluster_name)
        start_cluster(cluster_name)

    def destroy():
        p = subprocess.run([
            'flintrock', 'destroy', cluster_name, '--assume-yes'])
        assert p.returncode == 0
    request.addfinalizer(destroy)

    return cluster_name


@pytest.fixture(scope='module')
def stopped_cluster(request):
    cluster_name = 'running-cluster-' + random_string()
    p = subprocess.run([
        'flintrock', 'launch', cluster_name,
        '--num-slaves', '1',
        '--no-install-hdfs',
        '--no-install-spark',
        '--assume-yes',
        '--ec2-instance-type', 't2.small'])
    assert p.returncode == 0

    p = subprocess.run([
        'flintrock', 'stop', cluster_name, '--assume-yes'])
    assert p.returncode == 0

    def destroy():
        p = subprocess.run([
            'flintrock', 'destroy', cluster_name, '--assume-yes'])
        assert p.returncode == 0
    request.addfinalizer(destroy)

    return cluster_name


@pytest.fixture(scope='module')
def remote_file(request, running_cluster):
    """
    Return the path to a remote dummy file on a running Flintrock cluster.
    """
    file_path = '/tmp/remote_dummy_file_for_testing'
    p = subprocess.run([
        'flintrock', 'run-command', running_cluster, '--',
        'echo -e "{data}" > {path}'.format(
            data='test\n' * 3,
            path=file_path)])
    assert p.returncode == 0

    def destroy():
        p = subprocess.run([
            'flintrock', 'run-command', running_cluster, '--',
            'rm', '-f', file_path])
        assert p.returncode == 0
    request.addfinalizer(destroy)

    return file_path


@pytest.fixture(scope='module')
def local_file(request):
    """
    Return the path to a local dummy file.
    """
    file = tempfile.NamedTemporaryFile(delete=False)
    with open(file.name, 'wb') as f:
        f.truncate(1024)

    def destroy():
        os.remove(file.name)
    request.addfinalizer(destroy)

    return file.name
