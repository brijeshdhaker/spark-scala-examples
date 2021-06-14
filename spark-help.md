
docker exec spark-master /usr/local/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--deploy-mode cluster \
--conf spark.eventLog.dir=file:///d/apps/hostpath/spark/logs \
--class com.spark.tutorial.rdd.airports.AirportsInUsaProblem \
/usr/local/spark/work-dir/spark-scala-examples.jar %params%


file:/apps/hostpath/spark/in/word_count.text

val payments = sc.parallelize(Seq(
(1,101,2500),
(2,102,1110),
(3,103,500),
(4,104,400),
(5,105,150),
(6,106,450),
)).toDF("paymentId","customerId","amount")