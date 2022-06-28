from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd


def integrity_check(mydf, col1, col2):
    mydf.select(col1, col2).show()
    total_count = mydf.select(col1, col2).count()
    valid_count = mydf.na.drop(subset=[col1, col2]).count()
    bad_count = total_count - valid_count
    print("INTEGRITY OF COLUMNS: {} & {}".format(col1, col2))
    print("Percentage of Valid Records: {:.2f}%".format((valid_count / total_count) * 100))
    print("Percentage of Bad Records: {:.2f}%".format((bad_count / total_count) * 100))


def completeness_check(mydf, col1):
    mydf.select(col1).show()
    total_count = mydf.select(col1).count()
    null_chk_df = mydf.na.drop(subset=[col1])
    valid_count = null_chk_df.filter(col(col1) != 101).count()
    bad_count = total_count - valid_count
    print("COMPLETENESS OF COLUMN: {}".format(col1))
    print("Percentage of Valid Records: {:.2f}%".format((valid_count / total_count) * 100))
    print("Percentage of Bad Records: {:.2f}%".format((bad_count / total_count) * 100))


if __name__ == '__main__':
    # Step 01 : Create SparkSession
    spark = SparkSession.builder.master('local').appName("Data_Governance").getOrCreate()
    #
    # master_df = spark.read.csv(path="../../../../dataset/data/us_bb_browsing.csv", header=True)
    #
    # master_df.show()
    # integrity_check(master_df, "equip_id", "client_device")
    # completeness_check(master_df, "technology_id")
    xl_file = pd.read_excel("DataGovernance.xlsx", sheet_name="DG_Rules", header=1)
    xl_file.fillna("Null", inplace=True)
    xl_file = xl_file[["table name", "column name", "rule name", "custom expressions", 'thresholds']]
    print(xl_file)
    for i, row in xl_file.iterrows():
        new_dict = row.to_dict()
        table_name = new_dict["table name"]
        column_name = new_dict["column name"]
        rule_name = new_dict["rule name"]
        custom_expression = new_dict["custom expressions"]
        thresholds = new_dict["thresholds"]

        if table_name != "Null":
            file_path = "../../../../dataset/data/" + table_name
            master_df = spark.read.csv(path=file_path, header=True)
            total_count = mas
            completeness_check(master_df, column_name)
