set -e

cat << EOM > test.py
from pyspark import SparkContext
sc = SparkContext()
print "Slave Cores: {c}".format(c=sc.defaultParallelism)
EOM

./spark/bin/spark-submit test.py 2>&1 | grep "^Slave Cores"
