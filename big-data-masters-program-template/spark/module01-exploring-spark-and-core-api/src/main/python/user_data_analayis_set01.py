from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("User Data Analysis").getOrCreate()

    user_data_rdd = spark.sparkContext.textFile("../../../../dataset/u.user")

    print("First 5 record")

    for row in user_data_rdd.take(5):
        print(row)

    print("Total Count including header:{}".format(user_data_rdd.count()))

    print("********** Problem Statements *********************")

    print("Problem Statement 01")
    header = user_data_rdd.first()
    user_data_without_header = user_data_rdd.filter(lambda row: row not in header)
    for record in user_data_without_header.take(5):
        print(record)

    print("Problem Statement 02")
    rdd_with_designation = user_data_without_header.map(lambda x: x.split("|")[0] + "," + x.split("|")[3])
    for record in rdd_with_designation.take(5):
        print(record)

    print("Problem Statement 03")
    rdd_with_designation_tech = user_data_without_header.filter(lambda x: x.split("|")[3] == 'technician'). \
        map(lambda x: x.split("|")[0] + "," + x.split("|")[1] + "," + x.split("|")[3])
    for record in rdd_with_designation_tech.take(5):
        print(record)

    print("Problem Statement 04")
    rdd_with_designation_tech.saveAsTextFile("d:/rdd_output_2")
