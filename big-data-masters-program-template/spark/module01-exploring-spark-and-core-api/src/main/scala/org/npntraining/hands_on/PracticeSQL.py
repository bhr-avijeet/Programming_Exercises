from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("HandsOn2").getOrCreate()

data = [["google"], ["sun"], ["beach"], ["star"], ["nature"]]

googleDF = spark.createDataFrame(data, ["words"])

googleDF.printSchema()

googleDF.show(truncate=False)

# Find names with at least 2 vowels

search_pattern = "[aAeEiIoOuU]{2}"

search_pattern1 = "e$"

googleDF.filter(col("words").rlike(search_pattern1)).show()
