import json
import subprocess
import urllib.request

# TODO: Parallelize tests.


def test_describe_stopped_cluster(stopped_cluster):
    p = subprocess.run([
        './flintrock', 'describe', stopped_cluster],
        stdout=subprocess.PIPE)
    assert p.returncode == 0
    assert p.stdout.startswith(stopped_cluster.encode())


def test_try_launching_duplicate_stopped_cluster(stopped_cluster):
    p = subprocess.run([
        './flintrock', 'launch', stopped_cluster],
        stderr=subprocess.PIPE)
    assert p.returncode == 1
    assert p.stderr.startswith(b"Cluster already exists: ")


def test_try_launching_duplicate_cluster(running_cluster):
    p = subprocess.run([
        './flintrock', 'launch', running_cluster],
        stderr=subprocess.PIPE)
    assert p.returncode == 1
    assert p.stderr.startswith(b"Cluster already exists: ")


def test_describe_running_cluster(running_cluster):
    p = subprocess.run([
        './flintrock', 'describe', running_cluster],
        stdout=subprocess.PIPE)
    assert p.returncode == 0
    assert p.stdout.startswith(running_cluster.encode())


def test_run_command_on_running_cluster(running_cluster):
    p = subprocess.run([
        './flintrock', 'run-command', running_cluster, '--', 'ls', '-l'])
    assert p.returncode == 0


def test_copy_file_on_running_cluster(running_cluster, local_file):
    p = subprocess.run([
        './flintrock', 'copy-file', running_cluster, local_file, '/tmp/copied_from_local'])
    assert p.returncode == 0


def test_hdfs_on_running_cluster(running_cluster, remote_file):
    hdfs_path = '/hdfs_file'

    p = subprocess.run([
        './flintrock', 'run-command', running_cluster, '--master-only', '--',
        './hadoop/bin/hdfs', 'dfs', '-put', remote_file, hdfs_path])
    assert p.returncode == 0

    p = subprocess.run([
        './flintrock', 'run-command', running_cluster, '--',
        './hadoop/bin/hdfs', 'dfs', '-cat', hdfs_path])
    assert p.returncode == 0


def test_spark_on_running_cluster(running_cluster, remote_file):
    # TODO: Run a real query; e.g. sc.parallelize(range(10)).count()
    p = subprocess.run([
        './flintrock', 'run-command', running_cluster, '--',
        './spark/bin/pyspark', '--help'])
    assert p.returncode == 0

    p = subprocess.run([
        './flintrock', 'describe', running_cluster, '--master-hostname-only'],
        stdout=subprocess.PIPE)
    master_address = p.stdout.strip().decode('utf-8')
    assert p.returncode == 0

    spark_master_ui = 'http://{m}:8080/json/'.format(m=master_address)
    spark_ui_info = json.loads(
        urllib.request.urlopen(spark_master_ui).read().decode('utf-8'))
    assert spark_ui_info['status'] == 'ALIVE'


def test_operations_against_non_existent_cluster():
    cluster_name = 'this_cluster_doesnt_exist_yo'
    expected_error_message = b"No such cluster: "

    for command in ['describe', 'stop', 'start', 'login', 'destroy']:
        p = subprocess.run(
            ['./flintrock', command, cluster_name],
            stderr=subprocess.PIPE)
        assert p.returncode == 1
        assert p.stderr.startswith(expected_error_message)

    for command in ['run-command']:
        p = subprocess.run(
            ['./flintrock', command, cluster_name, 'ls'],
            stderr=subprocess.PIPE)
        assert p.returncode == 1
        assert p.stderr.startswith(expected_error_message)

    for command in ['copy-file']:
        p = subprocess.run(
            ['./flintrock', command, cluster_name, __file__, '/remote/path'],
            stderr=subprocess.PIPE)
        assert p.returncode == 1
        assert p.stderr.startswith(expected_error_message)
