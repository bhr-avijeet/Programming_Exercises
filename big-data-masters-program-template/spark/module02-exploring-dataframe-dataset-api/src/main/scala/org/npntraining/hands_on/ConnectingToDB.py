from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp"). \
        config("spark.jars", "../../../../../../../dataset/jars/sqlite-jdbc-3.36.0.3.jar").getOrCreate()

    connection_url = "jdbc:sqlite:C:/BigData/big-data-masters-program-template/spark/module02-exploring-dataframe" \
                     "-dataset-api/src/main/scala/org/npntraining/hands_on/spark-warehouse/window_functions.db"
    db_properties = {"driver": "org.sqlite.JDBC"}

    table_name = "employee_wf"

    employeeDF = spark.read.jdbc(url=connection_url, table=table_name, properties=db_properties)
    employeeDF.show()

    employeeDF.select(max(col("salary")), min(col("salary")), avg(col("salary")), sum(col("salary"))).show()
