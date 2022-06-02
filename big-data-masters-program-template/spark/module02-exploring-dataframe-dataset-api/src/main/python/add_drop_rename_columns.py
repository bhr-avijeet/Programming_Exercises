from pyspark.sql import SparkSession
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()
    user_df = spark.read.csv("../../../../dataset/u.user",header=True,sep="|")
    user_df.show()

    user_df.withColumn("ExpandedGender",when(col("gender") == "M", "Male"
                                            ).when(col("gender") == "F", "Female").otherwise("Transgender")

                       ).drop(col("gender")).select(col("designation"),col("salary").alias("sal"),col("ExpandedGender")).show()