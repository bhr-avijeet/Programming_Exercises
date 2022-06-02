from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").config("spark.sql.shuffle.partitions",
                                                                                "20").getOrCreate()

    crime_schema = StructType([
        StructField("code", StringType()),
        StructField("borough", StringType()),
        StructField("major_category", StringType()),
        StructField("minor_category", StringType()),
        StructField("value", IntegerType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
    ])

    crimeDataInputTable = spark.readStream.csv("../../../../dataset/crime_data/input",
                                               schema=crime_schema)

    # Demonstrating OutputMode : append
    # resultTable = crimeDataInputTable.filter(col("year") == 2012)
    # resultTable.writeStream.format("console").outputMode("append").start().awaitTermination()

    # Demonstrating OutputMode : Complete / Update
    resultTable = crimeDataInputTable.groupBy(col("borough")).agg(sum("value").alias("convictions"))
    resultTable.writeStream.format("console").outputMode("complete").start().awaitTermination()

    # Demonstrating SQL
    # crimeDataInputTable.createOrReplaceTempView("crime_data")
    # resultTable = spark.sql("select borough,sum(value) as convictions from crime_data group by borough")
    # resultTable.writeStream.format("console").outputMode("update").start().awaitTermination()
