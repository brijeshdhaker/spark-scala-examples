package com.scala.functions

object CurryingFunctions {

  // Define currying function
  def add(x: Int, y: Int) = x + y;

  // Curring function declaration
  def add2(a: Int) (b: Int) = a + b;

  def sum(a:Int) = (b:Int) => a+b

  def main(args: Array[String]){

    println(add(20,19))

    println(sum(20)(19))

    val s0 = sum(29);
    println(s0(19))

    // Partially Applied function.// Partially Applied function.
    val s1 = add2(29)_;
    println(s1(19))



  }

}
