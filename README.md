![Flintrock logo](flintrock-logo/v2/flintrock-logo.png)

Flintrock is a command-line tool and library for launching [Apache Spark](http://spark.apache.org/) clusters.

**Flintrock is currently undergoing heavy development. Until we make a 1.0 release, you probably should not use Flintrock unless you are ready to keep up with frequent changes to how it works.** Python hackers or heavy spark-ec2 users who are looking to experiment with something new are welcome to try Flintrock out and potentially [contribute](CONTRIBUTING.md).


## Usage

Here's a quick way to launch a cluster on EC2, assuming you already have an [AWS account set up](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html).

```sh
flintrock launch test-cluster \
    --num-slaves 1 \
    --ec2-key-name key_name \
    --ec2-identity-file /path/to/key.pem \
    --ec2-ami ami-146e2a7c
```

If you [persist these options to a file](#configurable-cli-defaults), you'll be able to do the same thing simply by typing:

```sh
flintrock launch test-cluster
```

Once you're done using a cluster, don't forget to destroy it with:

```sh
flintrock destroy test-cluster
```

And if you're lost, do try:

```sh
flintrock --help
flintrock <subcommand> --help
```

That's not all. Flintrock has a few more [features](#features) that you may find interesting.


## Installation

Flintrock requires Python 3.4 or newer.


## Use Cases

### Experimentation

If you want to play around with Spark, develop a prototype application, run a one-off job, or otherwise just experiment, Flintrock is the fastest way to get you a working Spark cluster.

### Performance testing

Flintrock exposes many options of its underlying providers (e.g. EBS-optimized volumes on EC2) which makes it easy to create a cluster with predictable performance for [Spark performance testing](https://github.com/databricks/spark-perf).

### Automated pipelines

Many people will use Flintrock interactively from the command line, but Flintrock can also be imported as a Python 3 library and used as part of an automated pipeline.


## Anti-Use Cases

There are some things that Flintrock specifically *does not* support.

### Managing permanent infrastructure

Flintrock is not for managing long-lived clusters, or any infrastructure that serves as a permanent part of some environment.

  For starters, Flintrock provides no guarantee that clusters launched with one version of Flintrock can be managed by another version of Flintrock, and no considerations are made for any long-term use cases.

  If you are looking for ways to manage permanent infrastructure, look at tools like [Terraform](https://www.terraform.io/), [Ansible](http://www.ansible.com/), [SaltStack](http://saltstack.com/), or [Ubuntu Juju](http://www.ubuntu.com/cloud/tools/juju). You might also find a service like [Databricks Cloud](https://databricks.com/product/databricks-cloud) useful if you're looking for someone else to host and manage Spark for you.

### Launching non-Spark-related services

Flintrock is meant for launching Spark clusters that include closely related services like HDFS, Mesos, and YARN.

  Flintrock is not for launching external datasources (e.g. Cassandra), or other services that are not closely integrated with Spark (e.g. Tez).

  If you are looking for an easy way to launch other services from the Hadoop ecosystem, look at the [Apache Bigtop](http://bigtop.apache.org/) and [Apache Whirr](https://whirr.apache.org/) projects.

### Launching out-of-date services

Flintrock will always take advantage of new features of Spark and related services to make the process of launching a cluster faster, simpler, and easier to maintain. If that means dropping support for launching older versions of a service, then we will generally make that tradeoff.


## Features

### Polished CLI

Flintrock has a clean command-line interface.

```sh
flintrock --help
flintrock describe
flintrock destroy --help
flintrock launch test-cluster --num-slaves 10
```

### Configurable CLI Defaults

Flintrock lets you persist your desired configuration to a file (called `config.yaml` by default) so that you don't have to keep typing out the same options over and over at the command line.

#### Sample `config.yaml`

```yaml
provider: ec2

launch:
  num-slaves: 1

ec2:
  key-name: key_name
  identity-file: /path/to/.ssh/key.pem
  instance-type: m3.medium
  region: us-east-1
  ami: ami-146e2a7c
```

With a config file like that, you can now launch a cluster with just this:

```sh
flintrock launch test-cluster
```

And if you want, you can even override individual options in your config file at the command line:

```sh
flintrock launch test-cluster \
    --num-slaves 10 \
    --ec2-instance-type r3.xlarge
```

### Fast Launches

Flintrock is really fast. This is how quickly it can launch fully operational clusters on EC2 compared to [`spark-ec2`](https://spark.apache.org/docs/latest/ec2-scripts.html).

* EC2 `m3.large` instances.
* Best of 6 tries.

| Cluster Size  | Flintrock Launch Time | `spark-ec2` Launch Time |
|---------------|----------------------:|------------------------:|
| 1 slave       | 2m 06s                |     8m 44s              |
| 50 slaves     | 5m 12s                |    37m 30s              |
| 100 slaves    | 8m 46s                | 1h 06m 05s              |

The `spark-ec2` launch times are sourced from [SPARK-5189](https://issues.apache.org/jira/browse/SPARK-5189).

### Low-level Provider Options

#### EC2

* EBS optimized
* etc.

### Library Use

Python 3.


## Anti-Features

### Support for out-of-date versions of Python, EC2, etc.

Supporting multiple versions of anything is tough. 

Same as anti-use cases. We are mortal beings with limited energy. People who support stuff across a wide cut of language or API versions are gods.


## Motivation

spark-ec2 was how I got started with Spark. It is one of the biggest reasons I found Spark so accessible.

Several limitations of [spark-ec2](https://spark.apache.org/docs/latest/ec2-scripts.html) provided the initial motivation for this project:

* **Slow launches**: spark-ec2 cluster launch times increase linearly with the number of slaves being created. For example, it takes spark-ec2 **[over an hour](https://issues.apache.org/jira/browse/SPARK-5189)** to launch a cluster with 100 slaves. (Flintrock can do it 5 minutes.) ([SPARK-4325](https://issues.apache.org/jira/browse/SPARK-4325), [SPARK-5189](https://issues.apache.org/jira/browse/SPARK-5189))
* **Immutable clusters**: Adding or removing slaves from an existing spark-ec2 cluster is not possible. ([SPARK-2008](https://issues.apache.org/jira/browse/SPARK-2008))
* **Out-of-date machine images**: spark-ec2 uses very old machine images, and the process of updating those machine images is not automated. A [bunch of work](https://issues.apache.org/jira/browse/SPARK-3821?focusedCommentId=14203280#comment-14203280) was done towards fixing that, but that work has now been adapted for use with Flintrock. ([SPARK-3821](https://issues.apache.org/jira/browse/SPARK-3821))
* **Unexposed EC2 options**: spark-ec2 does not expose all the EC2 options one would want to use as part of automated performance testing of Spark. ([SPARK-6220](https://issues.apache.org/jira/browse/SPARK-6220))
* **Poor support for programmatic use cases**: spark-ec2 was not built with programmatic use in mind, so many flows are difficult or impossible to automate. ([SPARK-5627](https://issues.apache.org/jira/browse/SPARK-5627), [SPARK-5629](https://issues.apache.org/jira/browse/SPARK-5629))
* **No support for configuration files**: spark-ec2 does not support reading options from a config file, so users are always forced to type them in at the command line. ([SPARK-925](https://issues.apache.org/jira/browse/SPARK-925))

Flintrock addresses all of these shortcomings.

### Additional Bonuses

* 1 request to allocate all instances -- no more bugs due to instance limits, metadata not propagating, etc.
* No assault on stdout during launch.
* Auth on client's IP address only, not 0.0.0.0/0.
