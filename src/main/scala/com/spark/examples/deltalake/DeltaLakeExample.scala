package com.spark.examples.deltalake

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._

object DeltaLakeExample extends App {

  // Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("DeltaLakeExample")
    .master("local[*]")
    .config("spark.sql.codegen.wholeStage", "false")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val deltaTable = DeltaTable.forPath("/tmp/delta-table")

  // Update every even value by adding 100 to it
  deltaTable.update(
    condition = expr("id % 2 == 0"),
    set = Map("id" -> expr("id + 100")))

  // Delete every even value
  deltaTable.delete(condition = expr("id % 2 == 0"))

  // Upsert (merge) new data
  val newData = spark.range(0, 20).toDF

  deltaTable.as("oldData")
    .merge(newData.as("newData"),"oldData.id = newData.id")
    .whenMatched
    .update(Map("id" -> col("newData.id")))
    .whenNotMatched
    .insert(Map("id" -> col("newData.id")))
    .execute()

  deltaTable.toDF.show()

}
