![Flintrock logo](flintrock-logo.png)

[![Build Status](https://travis-ci.org/nchammas/flintrock.svg)](https://travis-ci.org/nchammas/flintrock)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/nchammas/flintrock)

Flintrock is a command-line tool and library for launching [Apache Spark](http://spark.apache.org/) clusters.

**Flintrock is currently undergoing heavy development. Until we make a 1.0 release, you probably should not use Flintrock unless you are ready to keep up with frequent changes to how it works.** Python hackers or heavy spark-ec2 users who are looking to experiment with something new are welcome to try Flintrock out and potentially even [contribute](CONTRIBUTING.md).


## Usage

Here's a quick way to launch a cluster on EC2, assuming you already have an [AWS account set up](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html).

```sh
flintrock launch test-cluster \
    --num-slaves 1 \
    --no-install-hdfs \
    --spark-version 1.5.1 \
    --ec2-key-name key_name \
    --ec2-identity-file /path/to/key.pem \
    --ec2-ami ami-60b6c60a \
    --ec2-user ec2-user
```

If you [persist these options to a file](#configurable-cli-defaults), you'll be able to do the same thing simply by typing:

```sh
flintrock launch test-cluster
```

Once you're done using a cluster, don't forget to destroy it with:

```sh
flintrock destroy test-cluster
```

Other things you can do with Flintrock include:

```sh
flintrock login test-cluster
flintrock describe test-cluster
flintrock run-command test-cluster 'yum install -y package'
flintrock copy-file test-cluster /local/path /remote/path
```

To see what else Flintrock can do, or to see detailed help for a specific command, try:

```sh
flintrock --help
flintrock <subcommand> --help
```

That's not all. Flintrock has a few more [features](#features) that you may find interesting.


## Installation

Before using Flintrock, take a quick look at the [copyright](COPYRIGHT) notice and [license](LICENSE) and make sure you're OK with their terms.

Flintrock requires Python 3.4 or newer. Since we don't have any releases yet, the only way to install Flintrock at the moment is as follows:

```sh
# Download Flintrock.
git clone https://github.com/nchammas/flintrock

# Set your defaults.
cd flintrock
cp config.yaml.template config.yaml
# vi config.yaml

# Install Flintrock's dependencies.
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
deactivate

# You're good to go now.
./flintrock --help
```

Eventually, we plan to release binaries so that you can install Flintrock without having to worry about having Python installed.


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

  If you are looking for ways to manage permanent infrastructure, look at tools like [Terraform](https://www.terraform.io/), [Ansible](http://www.ansible.com/), [SaltStack](http://saltstack.com/), or [Ubuntu Juju](http://www.ubuntu.com/cloud/tools/juju). You might also find a service like [Databricks](https://databricks.com/product/databricks) useful if you're looking for someone else to host and manage Spark for you.

### Launching non-Spark-related services

Flintrock is meant for launching Spark clusters that include closely related services like HDFS, Mesos, and YARN.

  Flintrock is not for launching external datasources (e.g. Cassandra), or other services that are not closely integrated with Spark (e.g. Tez).

  If you are looking for an easy way to launch other services from the Hadoop ecosystem, look at the [Apache Bigtop](http://bigtop.apache.org/) project.

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

modules:
  spark:
    version: 1.5.1

launch:
  num-slaves: 1
  install-hdfs: False

ec2:
  key-name: key_name
  identity-file: /path/to/.ssh/key.pem
  instance-type: m3.medium
  region: us-east-1
  ami: ami-60b6c60a
  user: ec2-user
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

Flintrock is really fast. This is how quickly it can launch fully operational clusters on EC2 compared to [spark-ec2](https://spark.apache.org/docs/latest/ec2-scripts.html).

#### Setup

* Provider: EC2
* Instance type: `m3.large`
* AMI:
    * Flintrock: [Default Amazon Linux AMI](https://aws.amazon.com/amazon-linux-ami/)
    * spark-ec2: [Custom spark-ec2 AMI](https://github.com/amplab/spark-ec2/tree/a990752575cd8b0ab25731d7820a55c714798ec3/ami-list)
* Launch time: Best of 6 tries

#### Results

| Cluster Size  | Flintrock Launch Time |  spark-ec2 Launch Time  |
|---------------|----------------------:|------------------------:|
| 1 slave       | 2m 06s                |     8m 44s              |
| 50 slaves     | 5m 12s                |    37m 30s              |
| 100 slaves    | 8m 46s                | 1h 06m 05s              |

The spark-ec2 launch times are sourced from [SPARK-5189](https://issues.apache.org/jira/browse/SPARK-5189).

### Low-level Provider Options

#### EC2

Flintrock exposes low-level provider options (e.g. [instance-initiated shutdown behavior](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/terminating-instances.html#Using_ChangingInstanceInitiatedShutdownBehavior)) so you can control the details of how your cluster is setup if you want.

### Library Use

This is not yet proven out, but Flintrock's architecture should allow it to be used as a Python 3 library. Once the command-line application matures a bit, we will flesh out this use case if there is demand for it.


## Anti-Features

### Support for out-of-date versions of Python, EC2 APIs, etc.

Supporting multiple versions of anything is tough. There's more surface area to cover for testing, and over the long term the maintenance burden of supporting something non-current with bug fixes and workarounds really adds up.

There are projects that support stuff across a wide cut of language or API versions. For example, Spark supports Java 7 and 8, and Python 2.6+ and 3+. The people behind these projects are gods. They take on an immense maintenance burden for the benefit and convenience of their users.

We here at project Flintrock are much more modest in our abilities. We are best able to serve the project over the long term when we limit ourselves to supporting a small but widely applicable set of configurations.


## Motivation

*Note: The explanation here is provided from the perspective of Flintrock's original author, Nicholas Chammas.*

I got started with Spark by using [spark-ec2](http://spark.apache.org/docs/latest/ec2-scripts.html). It's one of the biggest reasons I found Spark so accessible. I didn't need to spend time upfront working through some setup guide before I could work on a "real" problem. Instead, with a simple spark-ec2 command I was able to launch a large, working cluster and get straight to business.

As I became a heavy user of spark-ec2, several limitations stood out and became an increasing pain. They provided me with the motivation for this project.

Among those limitations are:

* **Slow launches**: spark-ec2 cluster launch times increase linearly with the number of slaves being created. For example, it takes spark-ec2 **[over an hour](https://issues.apache.org/jira/browse/SPARK-5189)** to launch a cluster with 100 slaves. ([SPARK-4325](https://issues.apache.org/jira/browse/SPARK-4325), [SPARK-5189](https://issues.apache.org/jira/browse/SPARK-5189))
* **No support for configuration files**: spark-ec2 does not support reading options from a config file, so users are always forced to type them in at the command line. ([SPARK-925](https://issues.apache.org/jira/browse/SPARK-925))
* **Un-resizable clusters**: Adding or removing slaves from an existing spark-ec2 cluster is not possible. ([SPARK-2008](https://issues.apache.org/jira/browse/SPARK-2008))
* **Out-of-date machine images**: spark-ec2 uses very old machine images, and the process of updating those machine images is not automated. ([SPARK-3821](https://issues.apache.org/jira/browse/SPARK-3821))
* **Unexposed EC2 options**: spark-ec2 does not expose all the EC2 options one would want to use as part of automated performance testing of Spark. ([SPARK-6220](https://issues.apache.org/jira/browse/SPARK-6220))
* **Poor support for programmatic use cases**: spark-ec2 was not built with programmatic use in mind, so many flows are difficult or impossible to automate. ([SPARK-5627](https://issues.apache.org/jira/browse/SPARK-5627), [SPARK-5629](https://issues.apache.org/jira/browse/SPARK-5629))
* **No standalone distribution**: spark-ec2 comes bundled with Spark and has no independent releases or distribution. Instead of being a nimble tool that can progress independently and be installed separately, it is tied to Spark's release cycle and distributed with Spark, which clocks in at a few hundred megabytes.

Flintrock addresses, or will address, all of these shortcomings.

### Additional Bonuses

There are a few additional peeves I had with spark-ec2 -- some of which are difficult to fix -- that I wanted to address with Flintrock:

* Flintrock does not assault stdout with all kinds of unnecessary output during a cluster launch.
* By default, Flintrock only authorizes the client it's running from to SSH into launched clusters. spark-ec2 defaults to allowing anyone to make SSH attempts to the cluster.
* During an EC2 cluster launch Flintrock makes a single request to allocate all the required instances. This eliminates an annoying category of bugs in spark-ec2 related to hitting instance limits or to AWS metadata not propagating quickly enough.


## About the Flintrock Logo

The [Flintrock logo](flintrock-logo.png) was created using [Highbrow Cafetorium JNL](http://www.myfonts.com/fonts/jnlevine/highbrow-cafetorium/) and [this icon](https://thenounproject.com/term/stars/40856/). Licenses to use both the font and icon were purchased from their respective owners.
