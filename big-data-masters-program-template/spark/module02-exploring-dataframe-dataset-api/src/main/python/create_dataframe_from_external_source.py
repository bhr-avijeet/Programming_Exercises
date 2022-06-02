from pyspark.sql import SparkSession
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()

    print(type(spark.read))

    user_df = spark.read.csv(path="../../../../dataset/u.user",header=True,sep="|")

    user_df.show(100,False)

    user_df.select(col("id"),col("age")).show()

    user_df.filter((col("designation")=='technician') & (col("gender")=='M')).show()