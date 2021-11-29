#
$SPARK_HOME/bin/spark-shell

scala> :paste


# Run on a YARN cluster
export HADOOP_CONF_DIR=/home/brijeshdhaker/git-repos/spark-scala-examples/src/main/resources
./bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
--deploy-mode cluster \
--executor-memory 1G \
--num-executors 2 \
/opt/spark-2.3.1/examples/jars/spark-examples_2.11-2.3.1.jar \
4