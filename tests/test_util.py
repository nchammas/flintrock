from flintrock.util import spark_hadoop_build_version


def test_spark_hadoop_build_version():
    assert spark_hadoop_build_version('3.1.3') == 'hadoop3.2'
