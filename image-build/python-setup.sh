#!/bin/bash

# Prerequisites.
sudo yum install -y gcc

# Python 2.6
sudo yum install -y python26 python26-devel
sudo curl https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py | sudo python26
sudo easy_install pip
sudo pip2.6 install numpy
sudo pip2.6 install psutil

# Python 2.7
sudo yum install -y python27 python27-devel
sudo curl https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py | sudo python27
sudo easy_install-2.7 pip
sudo pip2.7 install numpy
sudo pip2.7 install psutil
