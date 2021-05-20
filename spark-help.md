
file:/E:/apps/hostpath/spark/in/word_count.text

val payments = sc.parallelize(Seq(
(1,101,2500),
(2,102,1110),
(3,103,500),
(4,104,400),
(5,105,150),
(6,106,450),
)).toDF("paymentId","customerId","amount")