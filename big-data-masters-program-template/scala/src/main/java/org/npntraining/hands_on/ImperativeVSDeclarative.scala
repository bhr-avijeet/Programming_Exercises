package org.npntraining.hands_on

import scala.collection.mutable.ListBuffer

object ImperativeVSDeclarative {
  def main(args: Array[String]): Unit = {
    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    var requiredNumbers = ListBuffer[Int]()


    // Imperative Style
    for (number <- numbers) {
      if (number % 2 == 0) {
        requiredNumbers += number
      }
    }

    println(requiredNumbers)

    println(numbers.filter(e=>e%2==0))

    var requriedNumbersAsList = for(number <- numbers if number%2==0) yield {
      number
    }

    println(requriedNumbersAsList)

  }
}

