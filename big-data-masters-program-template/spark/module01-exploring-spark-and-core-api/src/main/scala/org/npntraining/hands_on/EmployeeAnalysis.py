from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("HandsOn1").getOrCreate()

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

employeeDF = spark.read.csv(path="../../../../../../../dataset/data/employee.csv",
                            header=True, sep='|', schema=customSchema)

employeeDF.show(truncate=False)
employeeDF.printSchema()
print("Total count of records are {}".format(employeeDF.count()))
print("Columns:{}".format(employeeDF.columns))

# employeeDF.select("id", "name", "company").show(truncate=False)
# employeeDF.select("id", "name", "company").dropDuplicates(["company"]).show(truncate=False)
# employeeDF.select("id", "name", lower(col("company")).alias("company"))
# .dropDuplicates(["company"]).show(truncate=False)

# employeeDF.filter(lower(rtrim(ltrim(col("designation")))) != "team lead").show()

# employeeDF.filter(~col("designation").isin("R&D Engineer", "Developer")).show()
#
# employeeDF.createOrReplaceTempView("employee")
#
# spark.sql("select designation, max(salary) from employee group by designation").show()
#
# employeeDF.filter(col("designation") == "Developer").\
#     groupBy("gen").max("exp").withColumnRenamed("max(exp)", "Max_Exp").show()
#
# employeeDF.groupBy("designation").avg("salary").withColumnRenamed("avg(salary)","Average_Salary").\
#     sort(col("Average_Salary"), ascending=False).show()
# employeeDF.select(date_format(employeeDF.dob, "EEE, MMM d, ''yy").alias('Customized_Date')).show()


employeeDF.na.drop(subset=["id", "name"]).show()

employeeDF.na.fill("NULL IN SOURCE").show()
employeeDF.na.drop(subset=["id"]).na.fill(0, subset=["exp"]).na.fill("Anonymous", ["Name"]).show()
#
employeeDF.na.replace(1, 0, subset=["exp"]).show()
#