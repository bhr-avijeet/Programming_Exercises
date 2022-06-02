from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    # Step 01 : Create SparkSession
    spark = SparkSession.builder.master('local').appName("Employee Data Analysis_3").getOrCreate()

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
                                header=True, sep='|', schema=customSchema, quote="'")

    employeeDF.show(truncate=False)
    # employeeDF.printSchema()

    employeeDF1 = employeeDF.withColumn("skillss", split(col("skills"), ","))\
        .withColumn("salaries", split(col("previous_salary"), ","))

    employeeDF1.select(col("name"), col("skillss").getItem(0)).show()

    employeeDF1.withColumn("Actual_Salary", col("salaries").getItem(0).cast("integer"))\
        .withColumn("Expected_Salary", col("salaries").getItem(1).cast("integer").cast("integer")).show()

    employeeDF1.select(col("name"), array_max(col("salaries")).alias("Max_Salary")).show()

