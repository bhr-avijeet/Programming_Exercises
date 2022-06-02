package org.npntraining.hands_on

class Employee(var Name: String) {
  println("Constructor iNVOKED")
  Employee.incrementCounter()

}
object Employee{
  var counter: Int = 0

  def incrementCounter(): Unit = {
    counter += 1
  }

  def getCounterValue(): Int = {
    return counter
  }
}
object Test{
  def main(args: Array[String]): Unit = {
    val obj1 = new Employee("Naveen")
    val obj2 = new Employee("Nikshay")
    val obj3 = new Employee("Shruthi")
    val obj4 = new Employee("Sadhana")


    println(Employee.getCounterValue())
  }
}