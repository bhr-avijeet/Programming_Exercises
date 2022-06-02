package org.npntraining.hands_on

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.util.Properties

object AnalyticalFunctions {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()
    val dbProperties = new Properties()
    dbProperties.setProperty("driver", "org.sqlite.JDBC")

    val connectionURL = "jdbc:sqlite:d:/advance_sql.db"
    val employeeDF = spark.read
                          .jdbc(connectionURL, "employee_wf", dbProperties)


    val windowSpec1 = Window.partitionBy(col("department_id"))
                            .orderBy("years_worked")

    employeeDF.withColumn("lead", lead("salary", 1, 0).over(windowSpec1))
              .withColumn("lag", lag("salary", 1, 0).over(windowSpec1))
              .show()
  }
}