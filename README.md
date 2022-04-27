![Flintrock logo](https://raw.githubusercontent.com/nchammas/flintrock/master/flintrock-logo.png)

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/nchammas/flintrock/blob/master/LICENSE)
[![Build Status](https://github.com/nchammas/flintrock/actions/workflows/flintrock.yaml/badge.svg)](https://github.com/nchammas/flintrock/actions)

Flintrock is a command-line tool for launching [Apache Spark](http://spark.apache.org/) clusters.


## Flintrock around the web

Flintrock has been featured in a few talks, guides, and papers around the web.

* Talks:
  * [Flintrock: A faster, better spark-ec2](https://www.youtube.com/watch?v=3aeIpOGrJOA) ([slides](http://www.slideshare.net/SparkSummit/flintrock-a-faster-better-sparkec2-by-nicholas-chammas))
* Guides:
  * Running Spark on a Cluster: The Basics (using Flintrock)
    * [Part 1: Start a Spark Cluster and Use the spark-shell](http://heather.miller.am/blog/launching-a-spark-cluster-part-1.html)
    * [Part 2: Dependencies, S3, and Deploying via spark-submit](http://heather.miller.am/blog/launching-a-spark-cluster-part-2.html)    
  * [Spark with Jupyter on AWS](https://github.com/PiercingDan/spark-Jupyter-AWS)
  * [Building a data science platform for R&D, part 2 – Deploying Spark on AWS using Flintrock](https://alexioannides.com/2016/08/18/building-a-data-science-platform-for-rd-part-2-deploying-spark-on-aws-using-flintrock/)
  * [AWS EC2를 활용 스파크 클러스터 생성](http://statkclee.github.io/ml/ml-aws-ec2-flintrock.html)
* Papers:
  * ["Birds in the Clouds": Adventures in Data Engineering](https://arxiv.org/pdf/1710.08521.pdf)


## Usage

Here's a quick way to launch a cluster on EC2, assuming you already have an [AWS account set up](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html). Flintrock works best with Amazon Linux. You can get the latest AMI IDs [from here](https://aws.amazon.com/amazon-linux-2/release-notes/).

```sh
flintrock launch test-cluster \
    --num-slaves 1 \
    --spark-version 3.2.1 \
    --ec2-key-name key_name \
    --ec2-identity-file /path/to/key.pem \
    --ec2-ami ami-0aeeebd8d2ab47354 \
    --ec2-user ec2-user
```

If you [persist these options to a file](#configurable-cli-defaults), you'll be able to do the same thing much more concisely:

```sh
flintrock configure
# Save your preferences via the opened editor, then...
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
flintrock add-slaves test-cluster --num-slaves 2
flintrock remove-slaves test-cluster --num-slaves 1
flintrock run-command test-cluster 'sudo yum install -y package'
flintrock copy-file test-cluster /local/path /remote/path
```

To see what else Flintrock can do, or to see detailed help for a specific command, try:

```sh
flintrock --help
flintrock <subcommand> --help
```

That's not all. Flintrock has a few more [features](#features) that you may find interesting.

### Accessing data on S3

We recommend you access data on S3 from your Flintrock cluster by following
these steps:

1. Setup an [IAM Role](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html)
   that grants access to S3 as desired. Reference this role when you launch
   your cluster using the `--ec2-instance-profile-name` option (or its
   equivalent in your `config.yaml` file).
2. Reference S3 paths in your Spark code using the `s3a://` prefix. `s3a://` is
   backwards compatible with `s3n://` and replaces both `s3n://` and `s3://`.
   The Hadoop project [recommends using `s3a://`](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#S3A)
   since it is actively developed, supports larger files, and offers
   better performance.
3. Make sure Flintrock is configured to use Hadoop/HDFS 2.7+. Earlier
   versions of Hadoop do not have solid implementations of `s3a://`.
   Flintrock's default is Hadoop 3.3.2, so you don't need to do anything
   here if you're using a vanilla configuration.
4. Call Spark with the hadoop-aws package to enable `s3a://`. For example:
   ```sh
   spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2 my-app.py
   pyspark --packages org.apache.hadoop:hadoop-aws:3.3.2
   ```
   If you have issues using the package, consult the [hadoop-aws troubleshooting
   guide](http://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)
   and try adjusting the version. As a rule of thumb, you should match the version
   of hadoop-aws to the version of Hadoop that Spark was built against (which is
   typically Hadoop 3.2 or 2.7), even if the version of Hadoop that you're deploying to
   your Flintrock cluster is different.

With this approach you don't need to copy around your AWS credentials
or pass them into your Spark programs. As long as the assigned IAM role
allows it, Spark will be able to read and write data to S3 simply by
referencing the appropriate path (e.g. `s3a://bucket/path/to/file`).


## Installation

Before using Flintrock, take a quick look at the
[copyright](https://github.com/nchammas/flintrock/blob/master/COPYRIGHT)
notice and [license](https://github.com/nchammas/flintrock/blob/master/LICENSE)
and make sure you're OK with their terms.

**Flintrock requires Python 3.7 or newer**, unless you are using one
of our **standalone packages**. Flintrock has been thoroughly tested
only on OS X, but it should run on all POSIX systems.
A motivated contributor should be able to add
[Windows support](https://github.com/nchammas/flintrock/issues/46)
without too much trouble, too.

### Release version

To get the latest release of Flintrock, simply run [pip](https://pip.pypa.io/en/stable/):

```
pip3 install flintrock
```

This will install Flintrock and place it on your path. You should be good to go now!

You'll probably want to get started with the following two commands:

```sh
flintrock --help
flintrock configure
```

### Standalone version (Python not required!)

If you don't have a recent enough version of Python, or if you don't have Python installed at all,
you can still use Flintrock. We publish standalone packages of Flintrock on GitHub with our
[releases](https://github.com/nchammas/flintrock/releases).

Find the standalone package for your OS under our [latest release](https://github.com/nchammas/flintrock/releases/latest),
unzip it to a location of your choice, and run the `flintrock` executable inside.

For example:

```sh
flintrock_version="2.0.0"

curl --location --remote-name "https://github.com/nchammas/flintrock/releases/download/v$flintrock_version/Flintrock-$flintrock_version-standalone-macOS-x86_64.zip"
unzip -q -d flintrock "Flintrock-$flintrock_version-standalone-macOS-x86_64.zip"
cd flintrock/

# You're good to go!
./flintrock --help
```

You'll probably want to add the location of the Flintrock executable to your `PATH` so that you can invoke it from any directory.

### Community-supported distributions

Flintrock is also available via the following package managers:

* [Homebrew](https://brew.sh): `brew install flintrock`

These packages are not supported by the core contributors and **may be out of date**. Please reach out to the relevant communities directly if you have trouble using these distributions to install Flintrock. You can always find the latest release of Flintrock [on GitHub](https://github.com/nchammas/flintrock/releases/latest) and [on PyPI](https://pypi.org/project/Flintrock/).

### Development version

If you like living on the edge, install the development version of Flintrock:

```sh
pip3 install git+https://github.com/nchammas/flintrock
```

If you want to [contribute](https://github.com/nchammas/flintrock/blob/master/CONTRIBUTING.md), follow the instructions in our contributing guide on [how to install Flintrock](https://github.com/nchammas/flintrock/blob/master/CONTRIBUTING.md#contributing-code).

## Use Cases

### Experimentation

If you want to play around with Spark, develop a prototype application, run a one-off job, or otherwise just experiment, Flintrock is the fastest way to get you a working Spark cluster.

### Performance testing

Flintrock exposes many options of its underlying providers (e.g. EBS-optimized volumes on EC2) which makes it easy to create a cluster with predictable performance for [Spark performance testing](https://github.com/databricks/spark-perf).

### Automated pipelines

Most people will use Flintrock interactively from the command line, but Flintrock is also designed to be used as part of an automated pipeline. Flintrock's exit codes are carefully chosen; it offers options to disable interactive prompts; and when appropriate it prints output in YAML, which is both human- and machine-friendly.


## Anti-Use Cases

There are some things that Flintrock specifically *does not* support.

### Managing permanent infrastructure

Flintrock is not for managing long-lived clusters, or any infrastructure that serves as a permanent part of some environment.

  For starters, Flintrock provides no guarantee that clusters launched with one version of Flintrock can be managed by another version of Flintrock, and no considerations are made for any long-term use cases.

  If you are looking for ways to manage permanent infrastructure, look at tools like [Terraform](https://www.terraform.io/), [Ansible](http://www.ansible.com/), [SaltStack](http://saltstack.com/), or [Ubuntu Juju](http://www.ubuntu.com/cloud/tools/juju). You might also find a service like [Databricks](https://databricks.com/product/databricks) useful if you're looking for someone else to host and manage Spark for you. Amazon also offers [Spark on EMR](https://aws.amazon.com/elasticmapreduce/details/spark/).

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

Flintrock lets you persist your desired configuration to a YAML file so that you don't have to keep typing out the same options over and over at the command line.

To setup and edit the default config file, run this:

```sh
flintrock configure
```

You can also point Flintrock to a non-default config file by using the `--config` option.

#### Sample `config.yaml`

```yaml
provider: ec2

services:
  spark:
    version: 3.2.1

launch:
  num-slaves: 1

providers:
  ec2:
    key-name: key_name
    identity-file: /path/to/.ssh/key.pem
    instance-type: m5.large
    region: us-east-1
    ami: ami-0aeeebd8d2ab47354
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
    --ec2-instance-type r5.xlarge
```

### Fast Launches

Flintrock is really fast. This is how quickly it can launch fully operational clusters on EC2 compared to [spark-ec2](https://github.com/amplab/spark-ec2).

#### Setup

* Provider: EC2
* Instance type: `m3.large`
* AMI:
    * Flintrock: [Default Amazon Linux AMI](https://aws.amazon.com/amazon-linux-ami/)
    * spark-ec2: [Custom spark-ec2 AMI](https://github.com/amplab/spark-ec2/tree/a990752575cd8b0ab25731d7820a55c714798ec3/ami-list)
* Spark/Hadoop download source: S3
* Launch time: Best of 6 tries

#### Results

| Cluster Size  | Flintrock Launch Time |  spark-ec2 Launch Time  |
|---------------|----------------------:|------------------------:|
| 1 slave       | 2m 06s                |     8m 44s              |
| 50 slaves     | 2m 30s                |    37m 30s              |
| 100 slaves    | 2m 42s                | 1h 06m 05s              |

The spark-ec2 launch times are sourced from [SPARK-5189](https://issues.apache.org/jira/browse/SPARK-5189).

Note that AWS performance is highly variable, so you will not get these results consistently. They show the best case scenario for each tool, and not the typical case. For Flintrock, the typical launch time will be a minute or two longer.

### Advanced Storage Setup

Flintrock automatically configures any available [ephemeral storage](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html) on the cluster and makes it available to installed services like HDFS and Spark. This storage is fast and is perfect for use as a temporary store by those services.

### Tests

Flintrock comes with a set of automated, end-to-end [tests](https://github.com/nchammas/flintrock/tree/master/tests). These tests help us develop Flintrock with confidence and guarantee a certain level of quality.

### Low-level Provider Options

Flintrock exposes low-level provider options (e.g. [instance-initiated shutdown behavior](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/terminating-instances.html#Using_ChangingInstanceInitiatedShutdownBehavior)) so you can control the details of how your cluster is setup if you want.

### No Custom Machine Image Dependencies

Flintrock is built and tested against vanilla Amazon Linux and CentOS. You can easily launch Flintrock clusters using your own custom machine images built from either of those distributions.


## Anti-Features

### Support for out-of-date versions of Python, EC2 APIs, etc.

Supporting multiple versions of anything is tough. There's more surface area to cover for testing, and over the long term the maintenance burden of supporting something non-current with bug fixes and workarounds really adds up.

There are projects that support stuff across a wide cut of language or API versions. For example, Spark supports Java 7 and 8, and Python 2.6+ and 3+. The people behind these projects are gods. They take on an immense maintenance burden for the benefit and convenience of their users.

We here at project Flintrock are much more modest in our abilities. We are best able to serve the project over the long term when we limit ourselves to supporting a small but widely applicable set of configurations.


## Motivation

*Note: The explanation here is provided from the perspective of Flintrock's original author, Nicholas Chammas.*

I got started with Spark by using [spark-ec2](https://github.com/amplab/spark-ec2). It's one of the biggest reasons I found Spark so accessible. I didn't need to spend time upfront working through some setup guide before I could work on a "real" problem. Instead, with a simple spark-ec2 command I was able to launch a large, working cluster and get straight to business.

As I became a heavy user of spark-ec2, several limitations stood out and became an increasing pain. They provided me with the motivation for this project.

Among those limitations, the most frustrating ones were:

* **Slow launches**: spark-ec2 cluster launch times increase linearly with the number of slaves being created. For example, it takes spark-ec2 **[over an hour](https://issues.apache.org/jira/browse/SPARK-5189)** to launch a cluster with 100 slaves. ([SPARK-4325](https://issues.apache.org/jira/browse/SPARK-4325), [SPARK-5189](https://issues.apache.org/jira/browse/SPARK-5189))
* **No support for configuration files**: spark-ec2 does not support reading options from a config file, so users are always forced to type them in at the command line. ([SPARK-925](https://issues.apache.org/jira/browse/SPARK-925))
* **Un-resizable clusters**: Adding or removing slaves from an existing spark-ec2 cluster is not possible. ([SPARK-2008](https://issues.apache.org/jira/browse/SPARK-2008))
* **Custom machine images**: spark-ec2 uses custom machine images, making it difficult for users to bring their own image. And since the process of updating those machine images is not automated, they have not been updated in years. ([SPARK-3821](https://issues.apache.org/jira/browse/SPARK-3821))

I built Flintrock to address all of these shortcomings, which it does.

### Why build Flintrock when we have EMR?

I started work on Flintrock months before [EMR added support for Spark](https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/). It's likely that, had I considered building Flintrock a year later than I did, I would have decided against it.

Now that Flintrock exists, many users appreciate the lower cost of running Flintrock clusters as compared to EMR, as well as Flintrock's simpler interface. And for my part, I enjoy working on Flintrock in my free time.

### Why didn't you build Flintrock on top of an orchestration tool?

People have asked me whether I considered building Flintrock on top of Ansible, Terraform, Docker, or something else. I looked into some of these things back when Flintrock was just an idea in my head and decided against using any of them for two basic reasons:

1. **Fun**: I didn't have any experience with these tools, and it looked both simple enough and more fun to build something "from scratch".
2. **Focus**: I wanted a single-purpose tool with a very limited focus, not a module or set of scripts that were part of a sprawling framework that did a lot of different things.

These are not necessarily the right reasons to build "from scratch", but they were my reasons. If you are already comfortable with any of the popular orchestration tools out there, you may find it more attractive to use them rather than add a new standalone tool to your toolchain.


## About the Flintrock Logo

The [Flintrock logo](https://github.com/nchammas/flintrock/blob/master/flintrock-logo.png) was created using [Highbrow Cafetorium JNL](http://www.myfonts.com/fonts/jnlevine/highbrow-cafetorium/) and [this icon](https://thenounproject.com/term/stars/40856/). Licenses to use both the font and icon were purchased from their respective owners.
