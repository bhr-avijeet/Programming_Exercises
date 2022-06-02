from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp"). \
        config("spark.jars", "../../../lib/sqlite-jdbc-3.36.0.3.jar").getOrCreate()

    connectionURL = "jdbc:sqlite:d:/advance_sql.db"
    db_properties = {"driver": "org.sqlite.JDBC"}
    table_name = "employee_wf"
    employee_df = spark.read.jdbc(connectionURL, table_name, properties=db_properties)

    window_spec1 = Window.partitionBy(col("department_id"))
    employee_df.withColumn("running_total_sal_preceding", sum(col("salary")).over(window_spec1)).show()
