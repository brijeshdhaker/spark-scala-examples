package com.spark.examples.s3

import org.apache.spark.sql.SparkSession

object ParquetAWSExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("ParquetAWSExample")
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAVTXXM2UJB34ZN3EX")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  val data = Seq(("James ","Rose","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","Mary","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","1234","F",-1)
  )

  val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
  import spark.sqlContext.implicits._
  val df = data.toDF(columns:_*)

  df.show()
  df.printSchema()

  df.write
    .parquet("s3a://385992152338-s3-bucket-001/parquet/people.parquet")


  val parqDF = spark.read.parquet("s3a://385992152338-s3-bucket-001/parquet/people.parquet")
  parqDF.createOrReplaceTempView("ParquetTable")

  spark.sql("select * from ParquetTable where salary >= 4000").explain()
  val parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

  parkSQL.show()
  parkSQL.printSchema()

  df.write
    .partitionBy("gender","salary")
    .parquet("s3a://385992152338-s3-bucket-001/parquet/people2.parquet")
}
