import json
import subprocess
import urllib.request

# Flintrock modules
from flintrock.exceptions import ClusterInvalidState
from conftest import aws_credentials_required

pytestmark = aws_credentials_required


def test_describe_stopped_cluster(stopped_cluster):
    p = subprocess.run([
        'flintrock', 'describe', stopped_cluster],
        stdout=subprocess.PIPE)
    assert p.returncode == 0
    assert p.stdout.startswith(stopped_cluster.encode())


def test_stop_stopped_cluster(stopped_cluster):
    p = subprocess.run([
        'flintrock', 'stop', stopped_cluster],
        stdout=subprocess.PIPE)
    assert p.returncode == 0
    assert p.stdout == b"Cluster is already stopped.\n"


def test_try_launching_duplicate_stopped_cluster(stopped_cluster):
    p = subprocess.run([
        'flintrock', 'launch', stopped_cluster],
        stderr=subprocess.PIPE)
    assert p.returncode == 1
    assert p.stderr.decode('utf-8').startswith(
        "Cluster {c} already exists".format(c=stopped_cluster))


def test_start_running_cluster(running_cluster):
    p = subprocess.run([
        'flintrock', 'start', running_cluster],
        stdout=subprocess.PIPE)
    assert p.returncode == 0
    assert p.stdout == b"Cluster is already running.\n"


def test_try_launching_duplicate_cluster(running_cluster):
    p = subprocess.run([
        'flintrock', 'launch', running_cluster],
        stderr=subprocess.PIPE)
    assert p.returncode == 1
    assert p.stderr.decode('utf-8').startswith(
        "Cluster {c} already exists".format(c=running_cluster))


def test_describe_running_cluster(running_cluster):
    p = subprocess.run([
        'flintrock', 'describe', running_cluster],
        stdout=subprocess.PIPE)
    assert p.returncode == 0
    assert p.stdout.startswith(running_cluster.encode())


def test_run_command_on_running_cluster(running_cluster):
    p = subprocess.run([
        'flintrock', 'run-command', running_cluster, '--', 'ls', '-l'])
    assert p.returncode == 0


def test_copy_file_on_running_cluster(running_cluster, local_file):
    p = subprocess.run([
        'flintrock', 'copy-file', running_cluster, local_file, '/tmp/copied_from_local'])
    assert p.returncode == 0


def test_hdfs_on_running_cluster(running_cluster, remote_file):
    hdfs_path = '/hdfs_file'

    p = subprocess.run([
        'flintrock', 'run-command', running_cluster, '--master-only', '--',
        './hadoop/bin/hdfs', 'dfs', '-put', remote_file, hdfs_path])
    assert p.returncode == 0

    p = subprocess.run([
        'flintrock', 'run-command', running_cluster, '--',
        './hadoop/bin/hdfs', 'dfs', '-cat', hdfs_path])
    assert p.returncode == 0


def test_spark_on_running_cluster(running_cluster, remote_file):
    # TODO: Run a real query; e.g. sc.parallelize(range(10)).count()
    p = subprocess.run([
        'flintrock', 'run-command', running_cluster, '--',
        './spark/bin/pyspark', '--help'])
    assert p.returncode == 0

    p = subprocess.run([
        'flintrock', 'describe', running_cluster, '--master-hostname-only'],
        stdout=subprocess.PIPE)
    master_address = p.stdout.strip().decode('utf-8')
    assert p.returncode == 0

    spark_master_ui = 'http://{m}:8080/json/'.format(m=master_address)
    spark_ui_info = json.loads(
        urllib.request.urlopen(spark_master_ui).read().decode('utf-8'))
    assert spark_ui_info['status'] == 'ALIVE'


def test_operations_against_non_existent_cluster():
    cluster_name = 'this_cluster_doesnt_exist_yo'
    expected_error_message = (
        b"No cluster " + cluster_name.encode('utf-8') + b" in region ")

    for command in ['describe', 'stop', 'start', 'login', 'destroy']:
        p = subprocess.run(
            ['flintrock', command, cluster_name],
            stderr=subprocess.PIPE)
        assert p.returncode == 1
        assert p.stderr.startswith(expected_error_message)

    for command in ['run-command']:
        p = subprocess.run(
            ['flintrock', command, cluster_name, 'ls'],
            stderr=subprocess.PIPE)
        assert p.returncode == 1
        assert p.stderr.startswith(expected_error_message)

    for command in ['copy-file']:
        p = subprocess.run(
            ['flintrock', command, cluster_name, __file__, '/remote/path'],
            stderr=subprocess.PIPE)
        assert p.returncode == 1
        assert p.stderr.startswith(expected_error_message)


def test_operations_against_stopped_cluster(stopped_cluster):
    p = subprocess.run(
        ['flintrock', 'run-command', stopped_cluster, 'ls'],
        stderr=subprocess.PIPE)
    expected_error_message = str(
        ClusterInvalidState(
            attempted_command='run-command',
            state='stopped'))
    assert p.returncode == 1
    assert p.stderr.decode('utf-8').strip() == expected_error_message

    p = subprocess.run(
        ['flintrock', 'copy-file', stopped_cluster, __file__, '/remote/path'],
        stderr=subprocess.PIPE)
    expected_error_message = str(
        ClusterInvalidState(
            attempted_command='copy-file',
            state='stopped'))
    assert p.returncode == 1
    assert p.stderr.decode('utf-8').strip() == expected_error_message


def test_launch_with_bad_ami():
    p = subprocess.run([
        'flintrock', 'launch', 'whatever-cluster',
        '--ec2-ami', 'ami-badbad00'],
        stderr=subprocess.PIPE)
    assert p.returncode == 1
    assert p.stderr.startswith(b"Error: Could not find")
