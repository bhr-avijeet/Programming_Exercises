package org.npntraining.hands_on

object MethodsAndFunctions {

  def getArea(radius:Double):Double={
    val PI = 3.14
    radius * radius * PI
  }
  def main(args: Array[String]): Unit = {
    println(getArea(30))

    val getArea1 = (radius:Int)=>{
      val PI = 3.14
      PI * radius * radius
    }

    println(getArea1(3))
  }
}
