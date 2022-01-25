package com.scala.classes

class Point(xc: Int, yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
    println ("Point x location : " + x);
    println ("Point y location : " + y);
  }
}

class Location( val xc: Int, val yc: Int, val zc :Int) extends Point(xc, yc){
  var z: Int = zc

  def move(dx: Int, dy: Int, dz: Int) {
    x = x + dx
    y = y + dy
    z = z + dz
    println ("Point x location : " + x);
    println ("Point y location : " + y);
    println ("Point z location : " + z);
  }
}

object Demo {
  def main(args: Array[String]) {
    val loc = new Location(10, 20, 15);

    // Move to a new location
    loc.move(10, 10, 5);
  }
}
