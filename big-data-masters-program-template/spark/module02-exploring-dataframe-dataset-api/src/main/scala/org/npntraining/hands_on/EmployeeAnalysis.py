from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("EmployeeAnalysis").getOrCreate()

    customSchema = StructType([
        StructField("Name", StringType(), True),
        StructField("Skills", StringType(), True),
        StructField("Salary", StringType(), True),
    ])

    employeeDF = spark.read.csv(path="../../../../../../../dataset/data/employee_skills.csv",
                                header=True, schema=customSchema, quote="'")

    employeeDF.show(truncate=False)

    employeeDF1 = employeeDF.withColumn("Multiple Skills", split(col("Skills"), ',')).withColumn("Salaries",
                                                                                                 split(col("Salary"),
                                                                                                       ','))

    employeeDF1.select(col("name"), col("Multiple Skills").getItem(0).alias("Primary Skills"),
                       array_max(col("Salaries")).alias("Max Salary")).show()

    employeeDF1.filter(col("Salaries").getItem(0) > col("Salaries").getItem(1)) \
        .select(col("name"), col("Salaries").getItem(0).alias("Actual Salary"),
                col("Salaries").getItem(1).alias("Expected Salary")).show()
