from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()
    data = [(1,"Naveen"),(2,"SHruthi"),(3,"Nikshay")]
    columns = ["id","name"]
    df = spark.createDataFrame(data).toDF(*columns)

    df.printSchema()

    df.show()