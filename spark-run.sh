#!/bin/bash

set -e
set -x

export SPARK_HOME=/opt/spark-2.4.0

# We want YARN to use the Python from our virtual environment,
# which includes all our dependencies.
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_PYTHON="venv/bin/python"

#
# Run on a YARN cluster
#
export HADOOP_CONF_DIR=/home/brijeshdhaker/git-repos/spark-scala-examples/src/main/resources
export YARN_CONF_DIR=/home/brijeshdhaker/git-repos/spark-scala-examples/src/main/resources
#
#
#
$SPARK_HOME/bin/spark-submit \
--class com.spark.hdp.SparkOnHDPTest \
--master yarn \
--deploy-mode client \
--executor-memory 512MB \
--num-executors 2 \
--conf "spark.yarn.archive=hdfs:///user/brijeshdhaker/archives/spark-2.4.0.zip" \
/home/brijeshdhaker/git-repos/spark-scala-examples/target/spark-scala-examples.jar 2

