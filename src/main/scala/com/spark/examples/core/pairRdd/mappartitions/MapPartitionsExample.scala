package com.spark.examples.core.pairRdd.mappartitions

import com.spark.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}
/*
Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
   being the key and country name being the value. Then convert the country name to uppercase and
   output the pair RDD to out/airports_uppercase.text

   Each row of the input file contains the following columns:

   Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
   ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

   Sample output:

   ("Kamloops", "CANADA")
   ("Wewak Intl", "PAPUA NEW GUINEA")
   ...
 */

object MapPartitionsExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MapPartitionsExample").setMaster("local")
    val sc = new SparkContext(conf)

    val baseRdd = sc.parallelize(Seq(("1", 1), ("1", 2),("1", 2), ("3", 3), ("3", 4), ("1", 5), ("7", 7)))

    def myMapFunction(values : Iterator[(String, Int)]) : Iterator[String] = {
      println(s"invoked for ${values.toString()}")
      values.map(_._1)
    }

    // High Order Function
    val mapPartFun: (Iterator[(String, Int)]) => Iterator[String] = (values)=>{
      println(s"invoked for ${values.toString()}")
      values.map(_._1)
    }

    val resultRDD = baseRdd.mapPartitions(myMapFunction)
    for( e <- resultRDD.collect()) println(e)

    //upperCase.saveAsTextFile("out/airports_uppercase.text")

  }
}
