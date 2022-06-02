from pyspark.sql import SparkSession
from db_source import DBSource
if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp") \
        .config("spark.jars",
                "../../../lib/mysql-connector-java-8.0.28.jar").getOrCreate()

    # connection_url = "jdbc:sqlite:advance_sql.db"
    # db_properties = {"driver":"org.sqlite.JDBC"}
    # table_name = "employee"
    #
    # employee_df = spark.read.jdbc(url=connection_url, table=table_name, properties=db_properties)
    #
    # employee_df.show()
    #
    # employee_df.createOrReplaceTempView("employee")
    #
    # spark.sql("SELECT * from employee where salary>5").show()

    # connection_url = "jdbc:sqlite:advance_sql.db"
    # db_source = DBSource("org.sqlite.JDBC",connection_url,spark)
    # employee_df = db_source.get_df_from_sqlite("employee")
    # employee_df.show()

    connection_url ="jdbc:mysql://localhost:3307/testdb"
    db_source = DBSource("com.mysql.cj.jdbc.Driver",connection_url,spark)
    employee_df = db_source.get_df_from_sqlite("employee")
    employee_df.show()




