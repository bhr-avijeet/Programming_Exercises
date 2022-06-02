from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()

    customer_details_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("gender", StringType()),
        StructField("age", IntegerType())
    ])

    customer_details_df = spark.read.option("header", "true").schema(customer_details_schema).csv(
        "../../../../dataset/customer_transaction/static_datasets")

    customer_details_df.show()

    # customer_details_df = spark.read.csv(path="../../../../dataset/customer_transaction/static_datasets/",
    #                                      header=True, schema=customer_details_schema)

    customer_transaction_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("transaction_amount", IntegerType()),
        StructField("transaction_rating", IntegerType())
    ])

    customer_transaction_input_df = spark.readStream.option("header", "true").schema(customer_transaction_schema).csv(
        "../../../../dataset/customer_transaction/streaming_datasets/input")

    result_table = customer_details_df.join(customer_transaction_input_df,
                                            customer_details_df["customer_id"] == customer_transaction_input_df[
                                                "customer_id"])

    result_table.writeStream.format("console").outputMode("append").start().awaitTermination()
