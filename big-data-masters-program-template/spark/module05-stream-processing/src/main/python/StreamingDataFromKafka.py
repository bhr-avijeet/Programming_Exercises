from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp")\
        .config("spark.jars", "../../../../dataset/jars/spark-sql-kafka-0-10_2.11-2.4.0.jar,"
                              "../../../../dataset/jars/kafka-clients-1.1.0.jar").getOrCreate()

    inputTable = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option(
        "subscribe", "simple-producer-consumer").load()

    inputTable.printSchema()
    splitColumn = split(col("value"),",")
    resultTable = inputTable.select(col("value").cast(StringType()))\
                            .withColumn("event_time",splitColumn.getItem(0)) \
                            .withColumn("type",splitColumn.getItem(1)) \
                            .withColumn("amount",splitColumn.getItem(2)) \
                            .withColumn("stock_code",splitColumn.getItem(3)).drop("value")
    resultTable.writeStream.format("console").option("truncate", "false").outputMode(
        "update").start().awaitTermination()

