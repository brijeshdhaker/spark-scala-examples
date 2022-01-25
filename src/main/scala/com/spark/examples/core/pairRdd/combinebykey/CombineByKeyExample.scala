package com.spark.examples.core.pairRdd.combinebykey

import com.spark.examples.core.pairRdd.CarLoanApplicationAnalysis.result_rdd
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("CombineByKeyExample").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val list = List(("a", 1), ("b", 1), ("a", 2), ("a", 3), ("b", 0))
  val listRDD = sc.parallelize(list)

  def init(e:Int) : List[Int] = {
    // List of Integers
    val nums: List[Int] = List(e)
    return nums
  }

  def mergeValue(e:List[Int], x: Int) : List[Int] = {
    return e.::(x)
  }

  def mergeCombiner(a1:List[Int], a2:List[Int]) : List[Int] = {
   return a1.:::(a2)
  }

  val resultRDD = listRDD.combineByKey(init, mergeValue, mergeCombiner)
  resultRDD.collect().foreach(println)

}
