package org.npntraining.hands_on

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.IntegerType

object ProcessingJsonData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org")
          .setLevel(Level
            .ERROR);
    val spark = SparkSession.builder()
                            .appName("Crime Data Analysis")
                            .config("spark.sql.streaming.schemaInference", "true")
                            .master("local")
                            .getOrCreate()

    val schema = new StructType().add("firstName", StringType)
                                 .add("lastName", StringType)
                                 .add("isAlive", BooleanType)
                                 .add("age", IntegerType)
                                 .add("address", new StructType().add("city", StringType)
                                                                 .add("country", StringType)
                                                                 .add("countryCode", StringType)
                                 )

    val customerRawData = spark.readStream
                               .format("json")
                               .schema(schema)
                               .option("header", "false")
                               .option("path", "E:\\drop_location\\customer_data\\streaming_data\\")
                               .load()
    //    customerRawData.show()
    val result = customerRawData.withColumn("name", col("firstName"))
                                .withColumn("completeAddress",
                                  concat(
                                    col("address.city"),
                                    lit(","),
                                    col("address.country")
                                  )
                                )
                                .drop("firstName", "lastName", "isAlive", "age", "address")

    result.writeStream
          .format("csv")
          .option("path", "d:/dataset/nested_output/")
          .option("checkpointLocation", "d:/dataset/check")
          .option("header", "true")
          .outputMode(OutputMode.Append())
          .start()
          .awaitTermination()

  }
}