#!/bin/bash
# Creates an AMI for the Spark EC2 scripts starting with a stock Amazon 
# Linux AMI.
# This has only been tested with Amazon Linux AMI 2014.03.2 

set -e

if [ "$(id -u)" != "0" ]; then
   echo "This script must be run as root" >&2
   exit 1
fi

# Ganglia and misc tools
sudo yum install -y pssh git
sudo yum install -y xfsprogs

# Ganglia
sudo yum install -y \
  ganglia-3.6.0 \
  ganglia-web-3.6.0 \
  ganglia-gmond-3.6.0 \
  ganglia-gmetad-3.6.0

sudo yum install -y httpd24-2.4.10

# Install GNU parallel.
{
    pushd /tmp
    PARALLEL_VERSION="20141122"
    wget "http://ftpmirror.gnu.org/parallel/parallel-${PARALLEL_VERSION}.tar.bz2"
    bzip2 -dc "parallel-${PARALLEL_VERSION}.tar.bz2" | tar xvf -
    pushd "parallel-${PARALLEL_VERSION}"
    ./configure --prefix=/usr  # Amazon Linux root user doesn't have /usr/local on its $PATH
    make
    sudo make install
    popd
    rm -rf "./parallel-${PARALLEL_VERSION}*"
    popd

    # Suppress citation notice.
    echo "will cite" | parallel --bibtex
}

# Dev tools
sudo yum install -y java-1.7.0-openjdk-devel gcc gcc-c++ ant git
# Perf tools
sudo yum install -y dstat iotop strace sysstat htop perf

sudo yum --enablerepo='*-debug*' install -q -y java-1.7.0-openjdk-debuginfo.x86_64

# PySpark and MLlib deps
sudo yum install -y  python-matplotlib python-tornado scipy libgfortran
# SparkR deps
sudo yum install -y R

# Root ssh config
sudo sed -i 's/PermitRootLogin.*/PermitRootLogin without-password/g' \
  /etc/ssh/sshd_config
sudo sed -i 's/disable_root.*/disable_root: 0/g' /etc/cloud/cloud.cfg

# Set up ephemeral mounts
sudo sed -i 's/mounts.*//g' /etc/cloud/cloud.cfg
sudo sed -i 's/.*ephemeral.*//g' /etc/cloud/cloud.cfg
sudo sed -i 's/.*swap.*//g' /etc/cloud/cloud.cfg

echo "mounts:" >> /etc/cloud/cloud.cfg
echo " - [ ephemeral0, /mnt, auto, \"defaults,noatime,nodiratime\", "\
  "\"0\", \"0\" ]" >> /etc/cloud.cloud.cfg

for x in {1..23}; do
  echo " - [ ephemeral$x, /mnt$((x + 1)), auto, "\
    "\"defaults,noatime,nodiratime\", \"0\", \"0\" ]" >> /etc/cloud/cloud.cfg
done

# Install Maven (for Hadoop)
cd /tmp
maven_version="3.2.5"
wget "http://archive.apache.org/dist/maven/maven-3/${maven_version}/binaries/apache-maven-${maven_version}-bin.tar.gz"
tar xvzf "apache-maven-${maven_version}-bin.tar.gz"
mv "apache-maven-${maven_version}" /opt/

# Edit bash profile
echo "export PS1=\"\\u@\\h \\W]\\$ \"" >> ~/.bash_profile
echo "export JAVA_HOME=/usr/lib/jvm/java-1.7.0" >> ~/.bash_profile
echo "export M2_HOME=/opt/apache-maven-${maven_version}" >> ~/.bash_profile
echo "export PATH=\$PATH:\$M2_HOME/bin" >> ~/.bash_profile

source ~/.bash_profile

# Build Hadoop to install native libs
sudo mkdir /root/hadoop-native
cd /tmp
hadoop_version="2.4.1"
sudo yum install -y protobuf-compiler cmake openssl-devel
wget "http://archive.apache.org/dist/hadoop/common/hadoop-${hadoop_version}/hadoop-${hadoop_version}-src.tar.gz"
tar xvzf "hadoop-${hadoop_version}-src.tar.gz"
cd "hadoop-${hadoop_version}-src"
mvn package -Pdist,native -DskipTests -Dtar
sudo mv "hadoop-dist/target/hadoop-${hadoop_version}/lib/native/"* /root/hadoop-native

# Install Snappy lib (for Hadoop)
yum install -y snappy
ln -sf /usr/lib64/libsnappy.so.1 /root/hadoop-native/.

# Create /usr/bin/realpath which is used by R to find Java installations
# NOTE: /usr/bin/realpath is missing in CentOS AMIs. See
# http://superuser.com/questions/771104/usr-bin-realpath-not-found-in-centos-6-5
echo '#!/bin/bash' > /usr/bin/realpath
echo 'readlink -e "$@"' >> /usr/bin/realpath
chmod a+x /usr/bin/realpath
