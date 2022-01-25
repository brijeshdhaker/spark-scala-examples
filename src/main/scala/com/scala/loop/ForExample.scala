package com.scala.loop

object ForExample {

  def main(args: Array[String]) {
    var a = 0;
    val b = 0
    val numList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)


    // for loop execution with a range
    for( a <- 1 to 10){
      println( "Value of a: " + a );
    }

    // for loop execution with a range
    for( a <- 1 until 10){
      println( "Value of a: " + a );
    }

    // for loop execution with a range
    for( a <- 1 to 3; b <- 1 to 3){
      println( "Value of a: " + a );
      println( "Value of b: " + b );
    }

    // for loop execution with a collection
    for( a <- numList ){
      println( "Value of a: " + a );
    }

    // for loop execution with multiple filters
    for( a <- numList
         if a != 3; if a < 8 ){
      println( "Value of a: " + a );
    }

    // for loop execution with a yield
    var retVal = for{ a <- numList if a != 3; if a < 8 }yield a
    for( a <- retVal){
      println( "Value of a: " + a );
    }

  }
}
