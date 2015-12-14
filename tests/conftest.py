import os
import subprocess
import tempfile
import uuid

from collections import namedtuple

# External modules.
import pytest

HADOOP_VERSION = '2.7.1'
SPARK_VERSION = '1.5.2'


def random_string():
    return str(uuid.uuid4())[:8]


def launch_cluster(cluster_name, instance_type):
    p = subprocess.run([
        'flintrock', 'launch', cluster_name,
        '--num-slaves', '1',
        '--install-hdfs',
        '--hdfs-version', HADOOP_VERSION,
        '--install-spark',
        '--spark-version', SPARK_VERSION,
        '--spark-git-commit', '',
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


ClusterConfig = namedtuple('ClusterConfig', ['restarted', 'instance_type'])

cluster_configs = [
    ClusterConfig(restarted=False, instance_type='t2.small'),
    ClusterConfig(restarted=True, instance_type='t2.small'),
    ClusterConfig(restarted=False, instance_type='m3.medium'),
    ClusterConfig(restarted=True, instance_type='m3.medium')]


@pytest.fixture(
    scope='module',
    params=cluster_configs,
    ids=[str(cc) for cc in cluster_configs])
def running_cluster(request):
    """
    Return the name of a running Flintrock cluster.
    """
    cluster_name = 'running-cluster-' + random_string()
    launch_cluster(cluster_name, request.param.instance_type)

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
