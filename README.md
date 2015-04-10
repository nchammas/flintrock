# flintrock

flintrock is a command-line tool for launching Apache Spark clusters.

## Usage

```text
usage: flintrock [--version] [--help] [--log-level <level>]
                 [--config <path>]
                 [--provider <name>]
                 <command> [<args>]

<command>:
    launch          <cluster-name> [--slaves <num>]

                    [--install-spark|--no-install-spark]
                    [--spark-version <version>] [--spark-git-repo]

                    [--ec2-key-name <name>] [--ec2-identity-file <path>]
                    [--ec2-instance-type <type>]
                    [--ec2-region <name>]
                    [--ec2-availability-zone <name>]
                    [--ec2-ami <id>]
                    [--ec2-spot-price <price>]
                    [--ec2-vpc-id <id>] [--ec2-subnet-id <id>] [--ec2-placement-group <name>]
                    [--ec2-tenancy <type>] [--ec2-ebs-optimized|--ec2-no-ebs-optimized]
                    [--ec2-instance-initiated-shutdown-behavior <behavior>]

    destroy         <cluster-name> [--assume-yes]
                    [--ec2-delete-groups|--ec2-no-delete-groups]

    add-slaves      <cluster-name> <num>
                    [--ec2-identity-file <path>]

    remove-slaves   <cluster-name> <num> [--assume-yes]

    describe        <cluster-name>
                    [--master-hostname-only]

    login           <cluster-name>

    start           <cluster-name>

    stop            <cluster-name> [--assume-yes]
```

## Limited Scope

flintrock has a limited scope.

1. **Spark-centric**: If flintrock launches any service or tool other than Spark, it's strictly to support or integrate with Spark. flintrock is not for, say, launching a bare Mesos cluster.

2. **Suitable for programmatic use**: Many people will use flintrock interactively from the command line, but flintrock is also meant to be used as part of an automated job.

3. **Intended for short-lived clusters**: flintrock is for quickly spinning up a Spark cluster to test something out, run a job, or just experiment. It's also meant for creating Spark clusters with specific OS or network configurations for automated performance testing.

 flintrock is not for spinning up a production Spark cluster.

## Goals

1. Be a fun side-project.
2. Be a better alternative to spark-ec2 for the use-cases outlined above.

Right now, it seems unlikely that #2 will ever happen, but hey, I can dream right?

## Motivation

This project is inspired by [spark-ec2](https://spark.apache.org/docs/latest/ec2-scripts.html) and more generic service orchestration tools like [MIT StarCluster](http://star.mit.edu/cluster/) and [Ubuntu Juju](http://www.ubuntu.com/cloud/tools/juju). It also takes some UI cues from [Docker Machine](https://docs.docker.com/machine/).

Several limitations of spark-ec2 motivated this project:

* [SPARK-4325](https://issues.apache.org/jira/browse/SPARK-4325), [SPARK-5189](https://issues.apache.org/jira/browse/SPARK-5189): spark-ec2 cluster launch times increase linearly with the number of slaves being created. It takes spark-ec2 [over an hour](https://issues.apache.org/jira/browse/SPARK-5189) to launch a cluster with 100 slaves.
* [SPARK-2008](https://issues.apache.org/jira/browse/SPARK-2008): Adding or removing slaves from an existing cluster is not possible.
* [SPARK-3821](https://issues.apache.org/jira/browse/SPARK-3821): Updating the machine images spark-ec2 uses is not automated.
* [SPARK-6220](https://issues.apache.org/jira/browse/SPARK-6220): spark-ec2 does not expose all the EC2 options one would want to use as part of automated performance testing of Spark.
* [SPARK-925](https://issues.apache.org/jira/browse/SPARK-925): spark-ec2 does not allow options to be read from a config file.

flintrock addresses all of these shortcomings within the bounds of its scope.

### Additional Bonuses

* Programmatic use as library.
* No assault on stdout during launch.
* Auth on client's IP address only, not 0.0.0.0/0.

## Python 2 support

flintrock does not currently support Python 2 and will likely never do so. The main reasons for that are:

1. flintrock uses [AsyncSSH](https://github.com/ronf/asyncssh), which is built on top of Python 3.4's `asyncio` library. This gives us asynchronous SSH, which is essential for building a lightweight and fast tool that can orchestrate potentially hundreds of remote instances at once.
2. flintrock's dev team is really small. We can't support much outside of a narrow core set of features and environments. And if we have to choose between the old and the new, we will generally go with the new. This is a new project; there is little sense in building it on an old version of Python.

### Asynchronous SSH Libraries

* [AsyncSSH](https://github.com/ronf/asyncssh) is built on top of Python 3.4's `asyncio` library. Its API is a bit [low level](https://github.com/ronf/asyncssh/issues/10) and the library does [not have support for SFTP](https://github.com/ronf/asyncssh/issues/11), but it supports most of what we need well.
* [parallel-ssh](https://github.com/pkittenis/parallel-ssh) [relies on gevent](https://groups.google.com/d/msg/parallelssh/5m4N39no8O4/el4aYbiddjgJ), which is Python 2 only. There is an [open issue](https://github.com/gevent/gevent/issues/38) to make it compatible with Python 3.
* [Fabric](http://www.fabfile.org/) is not really well-suited to use as a library, and it provides asynchronous connections through multithreading, which is a big resource hog when connecting to hundreds of servers at once. Hopefully, [Fabric 2](http://www.fabfile.org/roadmap.html#invoke-fabric-2-x-and-patchwork) will address both these issues, but that's a while out.
* All other options that came to my attention did not appear to have much adoption or ongoing development. It would be a bad idea to rely on a random project that didn't seem actively maintained or used just because it promised async SSH on Python 3.
