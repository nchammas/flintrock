# Spark Image Build Scripts

These scripts use [Packer](http://www.packer.io/) to create and register a set of AMIs that includes all the software we need to quickly launch Spark clusters on EC2.

The generated AMIs cover the full deployment matrix supported by `spark-ec2`, which includes both AMI virtualization types and most EC2 regions. On completion, the build script updates the AMI IDs in `ami-list/` automatically.

These scripts use the [latest US East, EBS-backed Amazon Linux AMIs](http://aws.amazon.com/amazon-linux-ami/) as a base. The generated AMIs will be registered under the Amazon account associated with the AWS credentials set in the OS's environment.

In the future, these scripts may be extended to support generating Spark images on other platforms like Docker and GCE.

## Usage

Just call this script:

```
./build_spark_amis.sh
```

Note: You can call this script from any working directory and it will work.

## Generated AMIs

`build_spark_amis.sh` will create one EBS-backed AMI for every combination of the following attributes in parallel, for a total of 16 AMIs (1 × 2 × 8). Instance store AMIs are currently not covered.

### Base Spark AMI

Currently, we generate a single logical AMI that contains all of Spark's critical dependencies plus miscellaneous tools for `spark-ec2`.

This AMI includes:

  * Java 1.7
  * Python 2.7 and Python libraries like `numpy` and `scipy`
  * R
  * Maven
  * Ganglia
  * Useful tools like `pssh`

This AMI *does not* include:

* Spark
* Hadoop
* Tachyon

### AMI Virtualization Type

Both AMI virtualization types are covered.

1. Hardware Virtual Machine (HVM)
2. Paravirtual (PV)

### EC2 Region

Almost all [EC2 regions](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html) are covered.

1. `ap-northeast-1`
2. `ap-southeast-1`
3. `ap-southeast-2`
4. `eu-west-1`
5. `sa-east-1`
6. `us-east-1`
7. `us-west-1`
8. `us-west-2`

We currently don't support the `cn-north-1` ([SPARK-4241](https://issues.apache.org/jira/browse/SPARK-4241), [mitchellh/goamz#120](https://github.com/mitchellh/goamz/issues/120)) or `eu-central-1` ([SPARK-5398](https://issues.apache.org/jira/browse/SPARK-5398), [mitchellh/packer#1646](https://github.com/mitchellh/packer/issues/1646)) regions.
