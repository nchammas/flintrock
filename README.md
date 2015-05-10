# flintrock

flintrock is a command-line tool for launching [Apache Spark](http://spark.apache.org/) clusters.


## Usage

* config example
* full CLI example

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


## Use Cases

### Experimentation

If you want to play around with Spark, develop a prototype application, run a one-off job, or otherwise just experiment, flintrock is the fastest way to get you a working Spark cluster.

### Performance testing

flintrock exposes many options of its underlying providers (e.g. EBS-optimized volumes on EC2) which makes it easy to create a cluster with predictable performance for [Spark performance testing](https://github.com/databricks/auto-spark-perf).

### Automated pipelines

Many people will use flintrock interactively from the command line, but flintrock can also be imported as a Python library and used as part of an automated pipeline.


## Anti-Use Cases

There are some things that flintrock specifically does *not* support.

### Managing permanent infrastructure

flintrock is not for managing long-lived clusters, or any infrastructure that serves as a permanent part of some environment.

  For starters, flintrock provides no guarantee that clusters launched with one version of flintrock can be managed by another version of flintrock, and no considerations are made for any long-term use cases.

  If you are looking for ways to manage permanent infrastructure, look at tools like [Terraform](https://www.terraform.io/), [Ansible](http://www.ansible.com/), [SaltStack](http://saltstack.com/), or [Ubuntu Juju](http://www.ubuntu.com/cloud/tools/juju).

### Launching non-Spark-related services

flintrock is meant for launching Spark clusters that include closely related services like HDFS, Mesos, and YARN.

  flintrock is not for launching external datasources (e.g. Cassandra), or other services that are not closely integrated with Spark (e.g. Tez).

  If you are looking for an easy way to launch other services from the Hadoop ecosystem, look at the [Apache Bigtop](http://bigtop.apache.org/) and [Apache Whirr](https://whirr.apache.org/) projects.

### Launching out-of-date services

flintrock will always take advantage of new features of Spark and related services to make the process of launching a cluster faster, simpler, and easier to maintain. If that means dropping support for launching older versions of a service, then we will generally make that tradeoff.


## Features

* nice help, subcommands
* config file, overwrite at command line
* fast launches
* use as library

## Anti-Features



## Motivation

spark-ec2 was how I got started with Spark. It is one of the biggest reasons I found Spark so accessible.

Several limitations of [spark-ec2](https://spark.apache.org/docs/latest/ec2-scripts.html) provided the initial motivation for this project:

* **Slow launches**: spark-ec2 cluster launch times increase linearly with the number of slaves being created. For example, it takes spark-ec2 **[over an hour](https://issues.apache.org/jira/browse/SPARK-5189)** to launch a cluster with 100 slaves. (flintrock can do it 5 minutes.) ([SPARK-4325](https://issues.apache.org/jira/browse/SPARK-4325), [SPARK-5189](https://issues.apache.org/jira/browse/SPARK-5189))
* **Immutable clusters**: Adding or removing slaves from an existing spark-ec2 cluster is not possible. ([SPARK-2008](https://issues.apache.org/jira/browse/SPARK-2008))
* **Out-of-date machine images**: spark-ec2 uses very old machine images, and the process of updating those machine images is not automated. A [bunch of work](https://issues.apache.org/jira/browse/SPARK-3821) was done towards fixing that, but that work has now been adapted for use with flintrock. ([SPARK-3821](https://issues.apache.org/jira/browse/SPARK-3821))
* **Unexposed EC2 options**: spark-ec2 does not expose all the EC2 options one would want to use as part of automated performance testing of Spark. ([SPARK-6220](https://issues.apache.org/jira/browse/SPARK-6220))
* **Poor support for programmatic use cases**: spark-ec2 was not built with programmatic use in mind, so many flows are difficult or impossible to automate. ([SPARK-5627](https://issues.apache.org/jira/browse/SPARK-5627), [SPARK-5629](https://issues.apache.org/jira/browse/SPARK-5629))
* **No support for configuration files**: spark-ec2 does not support reading options from a config file, so users are always forced to type them in at the command line. ([SPARK-925](https://issues.apache.org/jira/browse/SPARK-925))

flintrock addresses all of these shortcomings.

### Additional Bonuses

* 1 request to allocate all instances -- no more bugs due to instance limits, metadata not propagating, etc.
* No assault on stdout during launch.
* Auth on client's IP address only, not 0.0.0.0/0.


## Internals FAQ

### Why no Python 2 support?

flintrock does not currently support Python 2 and will likely never do so. The main reasons for that are:

1. flintrock uses [AsyncSSH](https://github.com/ronf/asyncssh), which is built on top of Python 3.4's `asyncio` library. This gives us asynchronous SSH, which is essential for building a lightweight and fast tool that can efficiently orchestrate hundreds of remote instances at once.
2. flintrock's dev team is really small. We can't support much outside of a narrow core set of features and environments. And if we have to choose between the old and the new, we will generally go with the new. This is a new project; there is little sense in building it on an old version of Python.

### Asynchronous SSH Libraries

* [AsyncSSH](https://github.com/ronf/asyncssh) is built on top of Python 3.4's `asyncio` library. Its API is a bit [low level](https://github.com/ronf/asyncssh/issues/10) and the library does [not have support for SFTP](https://github.com/ronf/asyncssh/issues/11), but it supports most of what we need well.
* [parallel-ssh](https://github.com/pkittenis/parallel-ssh) [relies on gevent](https://groups.google.com/d/msg/parallelssh/5m4N39no8O4/el4aYbiddjgJ), which is Python 2 only. There is an [open issue](https://github.com/gevent/gevent/issues/38) to make it compatible with Python 3.
* [Fabric](http://www.fabfile.org/) is not really well-suited to use as a library, and it provides asynchronous connections through multithreading, which is a big resource hog when connecting to hundreds of servers at once. Hopefully, [Fabric 2](http://www.fabfile.org/roadmap.html#invoke-fabric-2-x-and-patchwork) will address both these issues, but that's a while out.
* All other options that came to my attention did not appear to have much adoption or ongoing development. It would be a bad idea to rely on a random project that didn't seem actively maintained or used just because it promised async SSH on Python 3.

### Why not use something like Salt/Ansible to provision and manage instances?

### Why not use Jinja for templating?
