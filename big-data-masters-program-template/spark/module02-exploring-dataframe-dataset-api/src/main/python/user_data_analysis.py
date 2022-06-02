from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()

    dfr = spark.read
    print(type(dfr))

    user_df = dfr.csv(path="../../../../dataset/u.user",
                      header=True,
                      sep="|",
                      inferSchema=True)
    print("Problem Statement 01a")
    print(user_df.count())

    print("Problem Statement 01a")
    user_df.show(n=100, truncate=False)

    print("Problem Statement 01-c")
    user_df.printSchema()

    print("Problem Statement 01-d")
    user_df.select(col("id"), col("age"), col("designation")).show(user_df.count())

    print("****** Problem Statements 02 ******")

    user_df.filter(col("designation") != 'other').show()

    user_df.withColumnRenamed("gen", "gender").show()

    df = user_df.withColumn("category", when(col("salary") < 30000, "Low")
                            .when((col("salary") > 30000) & (col("salary") < 100000), "Medium")
                            .otherwise("High")
                            )
    df.show(user_df.count())

    df.filter(col("category") == "High").select(col("designation"), col("salary")).show()

    print("Problem Statement 05")

    df.groupBy(col("category")).count().show()

    df.groupBy(col("category")).min("salary").show()

    df.groupBy(col("category")).max("salary").show()

    df.groupBy(col("category")).agg(
        sum("salary").alias("sum_of_salary"),
        min("salary").alias("minimum_salary"),
        max("salary").alias("maximum_of_salary"),
        count("salary").alias("count_of_salary")
    ).show()

    df.groupBy(col("category")).agg(
        sum("salary").alias("sum_of_salary"),
        min("salary").alias("minimum_salary"),
        max("salary").alias("maximum_of_salary"),
        count("salary").alias("count_of_salary")
    ).sort(col("sum_of_salary"), ascending=False).show()
