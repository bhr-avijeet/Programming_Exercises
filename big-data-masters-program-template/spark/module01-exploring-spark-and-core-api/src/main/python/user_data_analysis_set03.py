from pyspark.sql import SparkSession


def create_pair_rdd(record):
    fields = record.split("|")
    return (fields[3], int(fields[4]))


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()

    user_data_rdd = spark.sparkContext.textFile("../../../../dataset/u.user")
    header = user_data_rdd.first()
    user_data_without_header = user_data_rdd.filter(lambda row: row not in header)
    user_data_key_pair = user_data_without_header.map(lambda row: create_pair_rdd(row))

    for row in user_data_key_pair.collect():
        print(row)

    rdd = user_data_key_pair.reduceByKey(lambda x, y: x + y)
    for element in rdd.collect():
        print("element{}".format(element))
