from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()

    data = [
        (1,"Naveen12"),
        (2,"Avijeet12")
    ]
    df = spark.createDataFrame(data,["id","name"])
    df.show()

    # df.write.format("csv").option("sep","|").option("header","true").schema().save(path="c:/test/")

    df.write.csv(path="c:/test",mode="append",compression="gzip")