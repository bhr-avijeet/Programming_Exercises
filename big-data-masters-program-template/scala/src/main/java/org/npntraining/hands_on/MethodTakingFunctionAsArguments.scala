package org.npntraining.hands_on

object MethodTakingFunctionAsArguments {

  def halfMaker(number:Int):Double={
    number.toDouble/2
  }

  def addFive(number : Int):Int={
    number+5
  }

  def fn1(arg1:Int,arg2:Int):Int={
    return arg1+arg2;
  }

  def processRange(ananda:Int,aishwarya:Int,processor: Int=>AnyVal):Unit={
      for(i<-ananda to aishwarya){
        println(processor(i))
      }
  }

  def main(args: Array[String]): Unit = {
//    processRange(1,10,halfMaker)
//    processRange(1,10,fn1)

    processRange(1,10,(e)=>e+10)
  }
}
