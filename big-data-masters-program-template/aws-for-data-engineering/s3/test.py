from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()
    source = "s3://avijeet-bd-training/input/user.json"
    destination = "s3://avijeet-bd-training/output"
    userDF = spark.read.json(path=source)
    userDF.write.format('csv').mode("overwrite").save(destination)