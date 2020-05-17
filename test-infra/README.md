# Private VPC Test Infrastructure

The Terraform templates in this directory manage private VPC infrastructure that Flintrock contributors can use to test their changes.

## Set Up

To spin up a private VPC along with associated infrastructure like a NAT gateway:

```
terraform apply
```

You can provide the required variables to this command by creating a `terraform.tfvars` file. The variables you need to define are listed in `variables.tf`.

Once the `apply` command completes, you'll see some output like this:

```
Apply complete! Resources: 12 added, 0 changed, 0 destroyed.

Outputs:

bastion_ip = 18.205.7.24
```

SSH into your bastion host. You'll spin up Flintrock clusters from here. A virtual environment and Flintrock config file will already be setup for you based on the variables you provided to Terraform during infrastructure creation:

```sh
ssh ec2-user@18.205.7.24
source venv/bin/activate
less /home/ec2-user/.config/flintrock/config.yaml
```

All you need to do is pick a version of Flintrock to install and then you can begin your tests against a private VPC!

```sh
pip install https://github.com/nchammas/flintrock/archive/master.zip
flintrock launch test-cluster
flintrock login test-cluster
```

## Tear Down

A NAT gateway is expensive to keep up all the time, so you'll want to tear down the infrastructure when you're done. Be sure to first tear down any Flintrock clusters you launched into the test VPC.

```sh
./delete-test-infra.sh
```

This script calls `terraform destroy` after clearing out some infrastructure that Flintrock creates inside the private VPC.
