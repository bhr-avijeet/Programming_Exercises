from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("UserAnalysis").getOrCreate()
    user_df = spark.read.json("../../../../../../../dataset/data/user_1.json")

    user_df.show(truncate=False)

    user_df.printSchema()

    user_df.withColumn("Full_Address", concat(col("address.city"), lit("-"), col("address.countryCode"))).\
        select("firstName", "lastName", "full_address").show()
    