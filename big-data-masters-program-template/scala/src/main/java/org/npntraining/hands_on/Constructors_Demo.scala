package org.npntraining.hands_on

class AA(val a: Int,var b:Int) {

}

object Constructors_Demo {
  def main(args: Array[String]): Unit = {
    val obj = new AA(23, 24)
    println(obj
      .b)

    obj.b=100
    println(obj
      .b)


    val obj2 = new AA(100,100)
    println(obj2.a)

  }
}

