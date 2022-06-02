from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()

    data = [1,2,3,4,5,6,7,8,9,10]

    rdd = spark.sparkContext.parallelize(data)

    print(rdd.reduce(lambda x,y:x+y))