from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    # Step 01 : Create SparkSession
    spark = SparkSession.builder.master('local').appName("EmployeeDataAnalysis_0").getOrCreate()

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
    employeeDF = spark.read.csv(path='../../../../../../../dataset/data/employee.csv', header=True, sep='|', schema=customSchema)
    employeeDF.show(truncate=False)
    print('Total Count:{}'.format(employeeDF.count()))
    employeeDF.printSchema()
    print("Columns:{}".format(employeeDF.columns))
    employeeDF.select(col("name"), col("company").alias("company_name")).show()

    employeeDF.dropDuplicates(["company"]).show()

    employeeDF.select(lower(col("company")).alias("company")).dropDuplicates(["company"]).show(5)

    employeeDF.filter(ltrim(rtrim(col("designation"))) != 'Team Lead').show()

    employeeDF.select(rtrim(ltrim(lower(col("company")))).alias("company"), col("name")).filter(
        col("company") == 'cisco').show()

    employeeDF.filter(col("designation").isin(['Developer', 'R&D Engineer'])).show()

    employeeDF.createOrReplaceTempView("employee")

    spark.sql("select designation, avg(salary) from employee group by designation").show()

    employeeDF.filter(col('designation') == 'Developer').groupBy(employeeDF.gen).max('exp').withColumnRenamed('max(exp)', 'max_exp').show()

    employeeDF.groupBy(col("designation")).agg(avg("salary").alias("avg_salary")).sort(col("avg_salary"),
                                                                                       ascending=False).show()
    employeeDF.groupBy(employeeDF.designation).agg(avg('salary').alias("avg_salary")).\
        orderBy(desc('avg_salary')).show()

    employeeDF.select(lpad(col('gen'), 2, '#').alias('gen1')).show()
    employeeDF.select(employeeDF.columns[:4]).show()

    windowSpec = Window.partitionBy(rtrim(ltrim(lower(col("company"))))).orderBy('company')
    employeeDF.withColumn("row_number", row_number().over(windowSpec))\
        .withColumn("rank", rank().over(windowSpec)).show()

    employeeDF.select(col('name'), when(col('exp') > 7, 'Senior').otherwise('Junior').alias('Experience')).show()

    employeeDF.select(date_format(employeeDF.dob, "EEE, MMM d, ''yy").alias('Customized_Date')).show()



