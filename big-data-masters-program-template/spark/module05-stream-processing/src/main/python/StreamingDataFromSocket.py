from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()

    input_table = spark.readStream.format("socket").option("host","localhost").option("port","9901").load()

    streamingQuery = input_table.writeStream.format("console").outputMode("append").start()

    streamingQuery.awaitTermination()


