package org.npntraining.hands_on

object MethodsReturningAFunction {

  def method1(b: Boolean) = {
    if (b) {
      () => "The value is true"
    }
    else {
      () => "THe value is false"
    }
  }

  def main(args: Array[String]): Unit = {
      val fn = method1(true)

      println(fn())
  }
}
