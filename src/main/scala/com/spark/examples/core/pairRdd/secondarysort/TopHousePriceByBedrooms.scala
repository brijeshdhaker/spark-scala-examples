package com.spark.examples.core.pairRdd.secondarysort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/*
Create a Spark program to read the house data from in/RealEstate.csv,
output the average price for houses with different number of bedrooms.

The houses data set contains a collection of recent reale state listings in SanLuis Obispo county and
around it.

The data set contains the following fields:
1. MLS: Multiple listing Service number for the house(uniqueID).
2. Location: city/town where the house is located.
3. Price: the most recent listing price of the house(in dollars).
4. Bedrooms: Number of bedrooms.
5. Bathrooms: Number of bathrooms.
6. Size: Size of the house in square feet.
7. Price/SQ.ft: price of the house per square foot.
8. Status:type of sale.Thee type sarerepresented in the dataset: ShortSale,Foreclosureand Regular.

Each field is comma separated.

Sample output:

 (3, 325000)
 (1, 266356)
 (2, 325000)
 ...

 3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
*/

object TopHousePriceByBedrooms {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("TopHousePriceByBedrooms").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/datasets/RealEstate.csv").filter(line => !line.contains("Bedrooms")).map(line => line.split(","))
    val houses = lines.map(record => House(
      record(0),           //mslid
      record(1),           //location()
      record(2).toDouble,  //price
      record(3).toInt,     //bedroom
      record(4).toInt,     //bathroom
      record(5).toInt,     //size
      record(6).toDouble,  //psprice
      record(7)            //status
    ))

    //for ( house <- houses.collect()) println(house.bedrooms + " : " + house.location.trim)
    val locationHouseRDD = houses.map(h => (HouseKey(h.location.trim, h.bedrooms), 1)).reduceByKey((x,y) => x + y)
    for (record <- locationHouseRDD.take(20)) println(record._1.location+ " : "+ record._1.bedrooms + " : " + record._2)


    /*
     4. For each location, top House Prices which have 3 bedroom
     */


  }
}

//
case class House(mslid:String, location:String, price:Double, bedrooms:Int, bathrooms:Int, size:Int, price_sq_ft:Double, status:String )

//
case class HouseKey(location: String, bedrooms: Int)

//
class HousePartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[HouseKey]
    k.location.hashCode() % numPartitions
  }
}