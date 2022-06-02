from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *


def integrity_check(mydf, col1, col2):
    mydf.show()
    total_count = df.count()
    mydf.na.drop(subset=[col1, col2]).show()
    

if __name__ == '__main__':
    # Step 01 : Create SparkSession
    spark = SparkSession.builder.master('local').appName("Data_Governance").getOrCreate()

    df = spark.read.csv(path="../../../../dataset/data/us_bb_browsing.csv", header=True)

    df1 = df.select("equip_id", "client_device")

    integrity_check(df1, "equip_id", "client_device")
