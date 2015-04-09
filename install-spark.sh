#!/bin/bash -e

spark_version="$1"
distribution="$2"

echo "Installing Spark..."
echo "  version: ${spark_version}"
echo "  distribution: ${distribution}"

file="spark-${spark_version}-bin-${distribution}.tgz"

curl --silent --remote-name "http://s3.amazonaws.com/spark-related-packages/${file}"
mkdir "spark"
# strip-components puts the files in the root of spark/
tar xzf "$file" -C "spark" --strip-components=1
rm "$file"

sudo mkdir /mnt/spark
sudo chown ec2-user:ec2-user /mnt/spark

# if [[ "$SPARK_VERSION" == *\|* ]]
# then
#   mkdir spark
#   pushd spark > /dev/null
#   git init
#   repo=`python -c "print '$SPARK_VERSION'.split('|')[0]"` 
#   git_hash=`python -c "print '$SPARK_VERSION'.split('|')[1]"`
#   git remote add origin $repo
#   git fetch origin
#   git checkout $git_hash
#   sbt/sbt clean assembly
#   sbt/sbt publish-local
#   popd > /dev/null
# else 
#   if [[ "$HADOOP_MAJOR_VERSION" == "1" ]]; then
#     wget http://s3.amazonaws.com/spark-related-packages/spark-1.3.0-bin-hadoop1.tgz
#   else
#     wget http://s3.amazonaws.com/spark-related-packages/spark-1.3.0-bin-cdh4.tgz
#   fi

#   echo "Unpacking Spark"
#   tar xvzf spark-*.tgz > /tmp/spark-ec2_spark.log
#   rm spark-*.tgz
#   mv `ls -d spark-* | grep -v ec2` spark
# fi

# exit 0
