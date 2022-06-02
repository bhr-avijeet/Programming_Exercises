from pyspark.sql import SparkSession
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()
    user_df = spark.read.csv(path="s3://npntraining/user_data/input/", sep="|", header=True, inferSchema=True)
    result_df = user_df.groupBy(col("designation")).agg(sum("salary").alias("sum_of_salary"))
    result_df.repartition(1).write.mode("overwrite").csv("s3://npntraining/user_data/output1/")