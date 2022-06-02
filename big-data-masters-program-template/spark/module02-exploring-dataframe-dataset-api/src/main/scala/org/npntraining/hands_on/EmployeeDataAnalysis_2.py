from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    # Step 01 : Create SparkSession
    spark = SparkSession.builder.master('local').appName("Employee Data Analysis_2").getOrCreate()

    accessLogs = spark.read.option("mode", "PERMISSIVE").\
        option("columnNameOfCorruptRecord", "Bad_Record").\
        json(path='../../../../../../../dataset/data/access_logs.json')

    # accessLogs = spark.read.option("mode", "DROPMALFORMED").\
    # option("columnNameOfCorruptRecord", "Bad_Record").json(path='access_logs.json')

    # accessLogs = spark.read.option("mode", "FAILFAST").\
    # option("columnNameOfCorruptRecord", "Bad_Record").json(path='access_logs.json')

    accessLogs.show(truncate=False)