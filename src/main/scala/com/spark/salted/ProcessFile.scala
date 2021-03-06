package com.spark.salted

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object ProcessFile {

  def processTextRdd(textRdd: RDD[String]): Unit = {
    textRdd
      .map(txt => txt)
      .foreach(_ => {})
  }

  def processTextDf(textDataframe: DataFrame): Unit = {
    textDataframe
      .foreach(_ => {})
  }


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("Spark memory investigation")
      .master("local[3]")
      .getOrCreate()

    println("enter")

    val inputFile = "C:\\Neha\\personal\\Adobe\\Code\\spark_training-master\\data\\datamodeling\\generated_file_1_gb.txt"
    val textRdd: RDD[String] = session.sparkContext.textFile(inputFile)
    processTextRdd(textRdd)

    /*
      val textDF: DataFrame = session.read.text(inputFile)
      processTextDf(textDF)
    */

  }

}
