package com.scala.functions

object HighOrderFunctions {

  // Creating a list of numbers
  val num = List(7, 8, 9)

  // Defining a method
  def multiplyValue = (y: Int) => y * 3

  // A higher order function
  def apply(x: Double => String, y: Double) = x(y)

  // Defining a function for
  // the format and using a
  // method toString()
  def format[R](z: R) = "{" + z.toString() + "}"

  // Main method
  def main(args: Array[String]) {
    // Displays output by assigning
    // value and calling functions
    println(apply(format, 32))

    // Creating a higher order function
    // that is assigned to the variable
    val result = num.map(y => multiplyValue(y))

    // Displays output
    println("Multiplied List is: " + result)

  }
}
