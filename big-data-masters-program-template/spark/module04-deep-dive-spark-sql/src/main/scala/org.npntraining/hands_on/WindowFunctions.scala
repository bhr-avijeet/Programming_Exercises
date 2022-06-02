package org.npntraining.hands_on

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.util.Properties

object WindowFunctions {
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
                            .rowsBetween(Window
                              .unboundedPreceding, Window
                              .currentRow)

    val windowSpec2 = Window.partitionBy(col("department_id"))
                            .rowsBetween(Window
                              .unboundedPreceding, Window
                              .unboundedFollowing)

    val windowSpec3 = Window.partitionBy(col("department_id"))
                            .rowsBetween(-1,1)
    employeeDF.withColumn("running_total_sal_preceding", sum(col("salary")).over(windowSpec1))
              .withColumn("running_total_sal_preceding_following", sum(col("salary")).over(windowSpec2))
              .withColumn("running_total_sal_1preceding_1following", sum(col("salary")).over(windowSpec3))
              .show()
  }
}
