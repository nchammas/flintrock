#!/bin/bash

sudo sed --in-place -r "s/(^disable_root:) (true)/\1 false/g" /etc/cloud/cloud.cfg
sudo sed --in-place -r "0,/^\#PermitRootLogin/s/^\#(PermitRootLogin) (.*)/\1 without-password/g" /etc/ssh/sshd_config
