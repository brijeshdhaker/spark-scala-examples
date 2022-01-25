package com.spark.examples.core.pairRdd

import com.spark.commons.Utils
import com.spark.examples.core.pairRdd.CarLoanApplicationAnalysis.CarLoanCalulations
import com.spark.examples.core.pairRdd.CardTransactionAnalysis.{inputPairRDD, totalAmountByUser}
import com.spark.examples.core.pairRdd.aggregation.combinebykey.AverageHousePriceByBedrooms.AvgCount
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.{ListBuffer, Set}

/*
    Read the file from storage (/FileStore/tables/card_transactions.json)
    File has json records
    Each record has fields:
      amount
      card_num
      category
      merchant
      ts
      user_id

*/
object CarLoanApplicationAnalysis extends App {

  // Min, Max, Average, Total,
  type CarLoanCalulations = (Int, Int, Float, Double)

  //Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("CarLoanApplicationAnalysis").setMaster("local[3]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  /**
   * Analysis Datasource
   *
   */
  val lines = sc.textFile("/datasets/auto_loan.csv")
  val cleanedLines = lines.filter(line => !line.contains("application_id"))
  //cleanedLines.take(5).foreach(println)

  val rawPairRDD = cleanedLines.map(line => (
    line.split(Utils.COMMA_DELIMITER)(0),         // Application ID
    line.split(Utils.COMMA_DELIMITER)(1),         // Customer_ID
    line.split(Utils.COMMA_DELIMITER)(2).toInt,   // Amount
    line.split(Utils.COMMA_DELIMITER)(3),         // Car Model
    line.split(Utils.COMMA_DELIMITER)(4),         // Location
    line.split(Utils.COMMA_DELIMITER)(5),         // Loan Date
    line.split(Utils.COMMA_DELIMITER)(6)          // Loan Status
  ))
  //rawPairRDD.take(5).foreach(println)

  /*
   1. The month in which maximum loan requests were submitted in the last one year [2019-04-01 to 2020-03-31]
   */
  val resultPairRDD = rawPairRDD.filter(x => (("2019-04-01" <= x._6) && (x._6 < "2020-04-01"))).map(x => (x._6.split("-")(1), 1)).reduceByKey((x, y) => x + y)
  val countMonthRdd = resultPairRDD.map(x => (x._1.toInt, x._2.toInt))
ListBuffer
  println("The month in which maximum loan requests were submitted in the last one year [2019-04-01 to 2020-03-31] : ")
  //resultPairRDD.takeOrdered(12)(Ordering[Int].reverse.on(x => x._2)).foreach(println)
  val sortedRdd = countMonthRdd.sortBy[Int](_._2, false)
  //Just to display results from RDD
  sortedRdd.collect().toList.foreach(println)

  //
  var myfun1 = (s1: String, s2: String) => s1 + s2
  var myfun2 = (_: String) + (_: String)

  /*
   2. Max, Min and Average number of applications submitted per customer id
   */
  //1st Argument : specify the what to do with value of the key when the first time key appears in partition.
  def createCombiner(num_applications: Int): (Int, Int, Int, Int) = {
    return (num_applications, num_applications, num_applications, 1)
  }

  //2nd Argument : specify what to do with value of the key if the same key appears inside same partition
  // Min, Max, ApplicationCount, TotalCount
  def mergeValue(r: (Int, Int, Int, Int), num_applications: Int): (Int, Int, Int, Int) = {
    val min = r._1.min(num_applications)
    val max = r._2.max(num_applications)
    val total = r._3 + num_applications
    val count = r._4 + 1
    return (min, max, total, count)
  }

  //3rd Argument : specify what to do with the values of key across  other partitions
  def mergeCombiners(r1: (Int, Int, Int, Int), r2: (Int, Int, Int, Int)): (Int, Int, Int, Int) = {
    val min = r1._1.min(r2._1)
    val max = r1._2.max(r2._2)
    val total = r1._3 + r2._3
    val count = r1._4 + r2._4
    return (min, max, total, count)
  }

  // (customer_id, 1) --> (customer_id, total_application)
  val customerApplicationRDD = rawPairRDD.map(x => (x._2, 1)).reduceByKey((x, y) => x + y)

  // (customer_id, total_application) ---> (1, total_application)
  val numOfAppRDD = customerApplicationRDD.map(x => (1, x._2))
  val result_rdd = numOfAppRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)
  println()
  println("Max, Min and Average number of applications submitted per customer id")
  println()
  result_rdd.collect().foreach(println)
  val result = result_rdd.collect()(0)
  val (min_val, max_val, total, count) = result._2

  val avrage_val = (total % count).toFloat
  println(s"max: $max_val, min:$min_val, avg: $avrage_val")

  /*
   3. Top 10 highest car price against which applications got approved
   */
  val topCarPriceRDD = rawPairRDD.filter(_._7 == "approved").map(x => (x._3, x._1)).sortByKey(ascending = false)
  println()
  println()
  println("Top 10 highest car price against which applications got approved")
  topCarPriceRDD.take(10).foreach(println)

  /*
   4. For each customer location, top 5 car models which have most loan applications in the last month
  */
  // ((location, car_model),1) ---> ((location, car_model), total_count)
  val locationModelCountRDD = rawPairRDD.map(x => ((x._5, x._4), 1)).reduceByKey((x, y) => x + y)
  //locationModelCountRDD.take(10).foreach(println)
  val locationCountModelRDD = locationModelCountRDD.map(x => (CarLoanKey(x._1._1, x._2), x._1._2))
  //locationCountModelRDD.take(10).foreach(println)
  val sortedLocationCountModelRDD = locationCountModelRDD.repartitionAndSortWithinPartitions(new CarLoanPartitioner(1))

  val TOP_N = 5
  def getTopValues(records : Iterator[(CarLoanKey,String)]) : Iterator[(String,Int,String,Int)] = {
    var last_location : String = ""
    var current_rank = 0
    var list : List[(String, Int,String,Int)] = List()
    for(record <- records){
      var current_location = record._1.locationId
      var appliction_count = record._1.applications
      var car_model = record._2
      if(current_location != last_location) {
        last_location = current_location
        current_rank = 1
      }else{
        current_rank += 1
      }
      if(current_rank <=TOP_N){
        list = ((current_location, appliction_count, car_model, current_rank)) :: list
      }
    }
    return list.iterator
  }

  val top5ResultRDD = sortedLocationCountModelRDD.mapPartitions(getTopValues)
  println()
  println("For each customer location, top 5 car models which have most loan applications in the last month")
  println()
  top5ResultRDD.take(20).foreach(println)

  //Thread.sleep(3600000)

}

case class CarLoanApplication(airLine: String,
                              date: String,
                              originAirport: String,
                              originCity: String,
                              destAirport: String,
                              destCity: String,
                              arrivalDelay: Double) {
}

case class CarLoanKey(locationId: String, applications: Int)

object CarLoanKey {
  implicit def orderByApplications[A <: CarLoanKey]: Ordering[A] = {
    Ordering.by(clk => (clk.locationId, clk.applications * -1))
  }
}

class CarLoanPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[CarLoanKey]
    k.locationId.hashCode() % numPartitions
  }

}