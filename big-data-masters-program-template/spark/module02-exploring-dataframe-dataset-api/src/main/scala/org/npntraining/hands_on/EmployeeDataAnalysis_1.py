from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    # Step 01 : Create SparkSession
    spark = SparkSession.builder.master('local').appName("EmployeeDataAnalysis_1").getOrCreate()

    # Step 02 : Create a DataFrame
    customSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("exp", IntegerType(), True),
        StructField("gen", StringType(), True),
        StructField("dob", TimestampType(), True),
        StructField("company", StringType(), True),
        StructField("designation", StringType(), True),
        StructField("doj", StringType(), True),
        StructField("skills", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("previous_salary", StringType(), True)
    ])
    employeeDF = spark.read.csv(path='../../../../../../../dataset/data/employee.csv',
                                header=True, sep='|', schema=customSchema)

    employeeDF.show(truncate=False)

    employeeDF.withColumn("Experience_Level", when(col('exp') <= 4, "Junior")
                          .when(((col('exp') > 4) & (col('exp') <= 10)), "Mid Level")
                          .otherwise("Senior")
                          ).groupBy(col("Experience_Level")).agg(sum('salary')).show()

    employeeDF.withColumn("dojtemp", to_timestamp(col("doj"), 'dd-MM-YYYY')) \
        .drop("doj") \
        .withColumnRenamed("dojtemp", "doj") \
        .select(col("doj"), year(col("doj")).alias("year_of_joining")) \
        .select(datediff(current_timestamp(), col("doj")).alias("difference_in_date")).show()

    # employeeDF.show(truncate=False)
    # employeeDF.na.drop(subset=["id", "name"]).show()

    # employeeDF.na.fill("NULL IN SOURCE").show()
    employeeDF.na.drop(subset=["id"]).na.fill(0, subset=["exp"]).na.fill("Anonymous", ["Name"]).show()

    employeeDF.na.replace(1, 0, subset=["exp"]).show()