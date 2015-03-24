#!/bin/bash

set -e

sudo debuginfo-install -q -y kernel glibc
# sudo debuginfo-install -q -y glibc

# Both of these can be problematic.
# sudo yum update -y
# sudo yum update -y --security
