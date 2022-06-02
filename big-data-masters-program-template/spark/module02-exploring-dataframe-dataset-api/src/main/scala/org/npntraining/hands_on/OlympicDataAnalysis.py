from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("EmployeeDataAnalysis_4").getOrCreate()

    customSchema = StructType([
        StructField("Athlete", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Country", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Closing Date", StringType(), True),
        StructField("Sport", StringType(), True),
        StructField("Gold Medals", IntegerType(), True),
        StructField("Silver Medals", IntegerType(), True),
        StructField("Bronze Medals", IntegerType(), True),
        StructField("Total Medals", IntegerType(), True)
    ])

    olympicDF = spark.read.csv(path="../../../../../../../dataset/data/olympix_data.csv", header=True, sep=',', schema=customSchema, quote='"')

    olympicDF.show(truncate=False)

    olympicDF.filter(col("Sport") == "Swimming").groupBy(col("Country")). \
        agg(sum(col("Total Medals")).
            alias("Total Medals Won (Swimming)")).select(col("Country"), col("Total Medals Won (Swimming)")).show()

    olympicDF.filter(col("Country") == "India").groupBy(col("Year")). \
        agg(sum(col("Total Medals")).
            alias("Total Medals Won (By India)")).orderBy("Year").select(col("Year"),
                                                                        col("Total Medals Won (By India)")).show()

    olympicDF.groupBy(col("Country")).agg(sum(col("Total Medals")).alias("Total Medals Won")). \
        orderBy("Country").select(col("Country"), col("Total Medals Won")).show()

    olympicDF.groupBy(col("Country"), col("Year")). \
        agg(format_number(((sum("Gold Medals")/sum("Total Medals")) * 100), 2).alias("Percentage of Gold Medals"),
            format_number(((sum("Silver Medals")/sum("Total Medals")) * 100), 2).alias("Percentage of Silver Medals"),
            format_number(((sum("Bronze Medals")/sum("Total Medals")) * 100), 2).alias("Percentage of Bronze Medals"),
            sum("Total Medals").alias("Total Medals")).orderBy("Country", "Year").show()


