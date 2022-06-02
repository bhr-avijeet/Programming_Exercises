from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    custom_schema = StructType([
        StructField("facid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("membercost", FloatType(), True),
        StructField("guestcost", FloatType(), True),
        StructField("initialoutlay", FloatType(), True),
        StructField("monthlymaintenance", FloatType(), True)
    ])

    spark = SparkSession.builder.master("local").appName("FacilityDataAnalysis").getOrCreate()

    facilityDF = spark.read.csv(path="../../../../../../../dataset/data/Facilities.csv", header=True , schema=custom_schema)

    facilityDF.show()
    facilityDF.printSchema()

    facilityDF.filter(col("membercost") != 0.0).select(col("name")).show()

    free_facilities = facilityDF.filter(col("membercost") == 0.0).count()

    print(f'Number of Facilities which do not charge a fee to members: {free_facilities}')

    facilityDF.filter(col("membercost") < ((20/100)*col("monthlymaintenance"))).select(col("facid"), col("name"), col("membercost"), col("monthlymaintenance")).show()

    facilityDF.createOrReplaceTempView("facility")

    spark.sql("select * from facility where facid in (1, 5)").show()

    facilityDF.withColumn("affordability", when(col("monthlymaintenance") > 100.0, "Expensive").otherwise("Cheap")).\
        select(col("name"), col("monthlymaintenance"), col("affordability")).show()