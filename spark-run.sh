#!/bin/bash

set -e
set -x

# We want YARN to use the Python from our virtual environment,
# which includes all our dependencies.
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_PYTHON="venv/bin/python"

#
# Run on a YARN cluster
#
export SPARK_HOME=/opt/sandbox/spark-3.1.2
export HADOOP_CONF_DIR=/opt/sandbox/hadoop-3.2.1/etc/hadoop
export YARN_CONF_DIR=/opt/sandbox/hadoop-3.2.1/etc/hadoop

#
#
#
$SPARK_HOME/bin/spark-submit \
--name  spark-scala-example \
--master yarn \
--deploy-mode cluster \
--driver-memory 1024m \
--driver-cores 1 \
--num-executors 2 \
--executor-memory 640m \
--executor-cores 2 \
--class com.spark.SparkPi \
target/spark-scala-examples-1.0-SNAPSHOT.jar

