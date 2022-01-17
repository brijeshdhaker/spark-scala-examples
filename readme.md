## Source Data 
file:/D:/apps/hostpath/spark/in/word_count.text


# Minimum TODOs on a per job basis:
# 1. define name, application jar path, main class, queue and log4j-yarn.properties path
# 2. remove properties not applicable to your Spark version (Spark 1.x vs. Spark 2.x)
# 3. tweak num_executors, executor_memory (+ overhead), and backpressure settings

# the two most important settings:
num_executors=6
executor_memory=3g

# 3-5 cores per executor is a good default balancing HDFS client throughput vs. JVM overhead
# see http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
executor_cores=3

# backpressure
receiver_max_rate=100
receiver_initial_rate=30

#
#
mvn exec:exec@run-local -Drunclass=com.spark.SparkPi -Dparams="50"

mvn exec:exec@yarn-client -Drunclass=com.spark.SparkPi -Dparams="50"

mvn exec:exec@yarn-cluster -Drunclass=com.spark.SparkPi -Dparams="50"

#
#
# Run on a YARN cluster
export HADOOP_CONF_DIR=/opt/sandbox/hadoop-3.2.1/etc/hadoop
export YARN_CONF_DIR=/opt/sandbox/hadoop-3.2.1/etc/hadoop

#
spark-submit \
--name  spark-scala-example \
--master local[4] \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
--class com.spark.streaming.structure.KafkaStructuredStream \
target/spark-scala-examples-1.0-SNAPSHOT.jar

#
spark-submit \
--name  spark-scala-example \
--master yarn \
--deploy-mode cluster \
--driver-memory 1024m \
--driver-cores 1 \
--num-executors 2 \
--executor-memory 640m \
--executor-cores 2 \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
--class com.spark.streaming.structure.KafkaStructuredStream \
target/spark-scala-examples-1.0-SNAPSHOT.jar

#
#
#
spark-submit --master yarn --deploy-mode cluster \
--name <my-job-name> \
--class <main-class> \
--driver-memory 2g \
--num-executors ${num_executors} 
--executor-cores ${executor_cores} 
--executor-memory ${executor_memory} \
--queue <realtime_queue> \
--files <hdfs:///path/to/log4j-yarn.properties> \
--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j-yarn.properties \
--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j-yarn.properties \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer `# Kryo Serializer is much faster than the default Java Serializer` \
--conf spark.locality.wait=10 `# Increase job parallelity by reducing Spark Delay Scheduling (potentially big performance impact (!)) (Default: 3s)` \
--conf spark.task.maxFailures=8 `# Increase max task failures before failing job (Default: 4)` \
--conf spark.ui.killEnabled=false `# Prevent killing of stages and corresponding jobs from the Spark UI` \
--conf spark.logConf=true `# Log Spark Configuration in driver log for troubleshooting` \
`# SPARK STREAMING CONFIGURATION` \
--conf spark.streaming.blockInterval=200 `# [Optional] Tweak to balance data processing parallelism vs. task scheduling overhead (Default: 200ms)` \
--conf spark.streaming.receiver.writeAheadLog.enable=true `# Prevent data loss on driver recovery` \
--conf spark.streaming.backpressure.enabled=true \
--conf spark.streaming.backpressure.pid.minRate=10 `# [Optional] Reduce min rate of PID-based backpressure implementation (Default: 100)` \
--conf spark.streaming.receiver.maxRate=${receiver_max_rate} `# [Spark 1.x]: Workaround for missing initial rate (Default: not set)` \
--conf spark.streaming.kafka.maxRatePerPartition=${receiver_max_rate} `# [Spark 1.x]: Corresponding max rate setting for Direct Kafka Streaming (Default: not set)` \
--conf spark.streaming.backpressure.initialRate=${receiver_initial_rate} `# [Spark 2.x]: Initial rate before backpressure kicks in (Default: not set)` \
`# YARN CONFIGURATION` \
--conf spark.yarn.driver.memoryOverhead=512 `# [Optional] Set if --driver-memory < 5GB` \
--conf spark.yarn.executor.memoryOverhead=1024 `# [Optional] Set if --executor-memory < 10GB` \
--conf spark.yarn.maxAppAttempts=4 `# Increase max application master attempts (needs to be <= yarn.resourcemanager.am.max-attempts in YARN, which defaults to 2) (Default: yarn.resourcemanager.am.max-attempts)` \
--conf spark.yarn.am.attemptFailuresValidityInterval=1h `# Attempt counter considers only the last hour (Default: (none))` \
--conf spark.yarn.max.executor.failures=$((8 * ${num_executors})) `# Increase max executor failures (Default: max(numExecutors * 2, 3))` \
--conf spark.yarn.executor.failuresValidityInterval=1h `# Executor failure counter considers only the last hour` \
</path/to/spark-application.jar>

## Data for Spark Join 
val payments = sc.parallelize(Seq(
    (1,101,2500),
    (2,102,1110),
    (3,103,500),
    (4,104,400),
    (5,105,150),
    (6,106,450)
)).toDF("paymentId","customerId","amount")

val customers  = sc.parallelize(Seq(
(101,"Jon"),
(102,"Aron"),
(103,"Sam")
)).toDF("customerId","name")

## 1. Inner Join
val innerJoinDf = customers.join(payments, Seq("custometId"), "inner")

## 2. Left Join --> left | leftouter | left_outer
val leftJoinDf = payments.join(customers, Seq("customerId"),"left")
val leftJoinDf = payments.join(customers, Seq("customerId"),"leftouter")
val leftJoinDf = payments.join(customers, Seq("customerId"),"left_outer")

## 3. Right Join --> right | rightouter | right_outer
val rightJoinDf = payments.join(customers, Seq("customerId"),"right")
val rightJoinDf = payments.join(customers, Seq("customerId"),"rightouter")
val rightJoinDf = payments.join(customers, Seq("customerId"),"right_outer")

## 4. Outer Join | Full Join --> outer | full | fullouter
val outerJoinDf = payments.join(customers, Seq("customerId"),"outer")
outerJoinDf.show()

## 5. Cross Join
val crossJoinDf = customers.crossJoin(payments)
crossJoinDf.show()

## 6. Left Semi Join
val leftsemiJoinDf = payments.join(customers,payments("customerId")===customers("customerId"),"leftsemi")
leftsemiJoinDf.show()

## 7. Left Anti Join
val leftantiJoinDf = payments.join(customers,payments("customerId")===customers("customerId"),"leftanti")
leftantiJoinDf.show()

## 8. Self Join

val employees =   spark.createDataFrame(Seq(
    (1,"CEO",None),
    (2,"Manager-1",Some(1)),
    (3,"Manager-2",Some(1)),
    (101,"Team-Member-1",Some(2)),
    (102,"Team-Member-2",Some(2)),
    (103,"Team-Member-2",Some(2)),
    (201,"Team-Member-1",Some(3)),
    (202,"Team-Member-2",Some(3))
)).toDF("employeeId","employeeName","managerId")

val selfJoinDf = employees.as("E").join(employees.as("M"),$"M.employeeId" === $"E.managerId")
selfJoinDf.show()
selfJoinDf.select($"E.employeeName".as("Employee"),$"M.employeeName".as("Manager Name")).show()


## Spark Hive
scala>
scala> spark.conf.get("spark.sql.catalogImplementation")
scala> spark.catalog.listTables.show
scala> spark.sharedState.externalCatalog.listTables("userdb")
scala> spark.table("userdb.employee").show


./sbin/start-thriftserver.sh \
  --hiveconf hive.server2.thrift.port=spark-master \
  --hiveconf hive.server2.thrift.bind.host=10000 \
  --master spark://spark-master:7077
  --conf hive.metastore.uris=thrift://spark-master:10000

$SPARK_HOME/bin/spark-shell.cmd --master spark://spark-master:7077 

$SPARK_HOME/bin/spark-shell --master spark://spark-master:7077
$SPARK_HOME/bin/spark-sql