package org.npntraining.hands_on

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.util.Properties

object SQLiteSparkConnection {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()


    val dbProperties = new Properties()
    dbProperties.setProperty("driver","org.sqlite.JDBC")

    val connectionURL = "jdbc:sqlite:d:/advance_sql.db"
    val employeeDF = spark.read.jdbc(connectionURL,"employee_wf",dbProperties)


    employeeDF.createOrReplaceTempView("employee_wf")
    spark.sql("SELECT *, sum(salary) OVER(PARTITION by department_id) from employee_wf ").show()

  }
}
