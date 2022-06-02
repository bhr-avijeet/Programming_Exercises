from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("LoanDataAnalysis").getOrCreate()

    LoanDataAnalysisDF = spark.read.csv(path="../../../../../../../dataset/data/LoanStats_2018Q4.csv", header=True, sep=',')

    LoanDataAnalysisDF.show()

    columns_list = [columns for columns in LoanDataAnalysisDF.columns]

    print(columns_list)

    LoanDataAnalysisDF.select("term", "home_ownership", "grade", "purpose", "int_rate",
                              "Installment", "addr_state", "loan_status", "application_type", "loan_amnt", "emp_length",
                              "annual_inc", "dti", "delinq_2yrs",
                              "revol_bal", "revol_util", "total_acc", "num_tl_90g_dpd_24m", "dti_joint").show()

    LoanDataAnalysisDF.agg(max("loan_amnt").alias("Max_Loan_Amount"), min("loan_amnt").alias("Min_Loan_Amount"),
                           avg("loan_amnt").alias("Average_Loan_Amount")).show()

    LoanDataAnalysisDF.describe("loan_amnt").show()

    LoanDataAnalysisDF.select("loan_amnt", "annual_inc").summary("min", "max", "mean", "stddev").show()

    regex_string = "years|year|\\<|\\+"

    regex_string1 = "years?|\\W"

    LoanDataAnalysisDF.select(regexp_replace(col("emp_length"), regex_string1, "").alias("cleansed_emp_length")).\
        select((ltrim(rtrim(col("cleansed_emp_length")))).alias("cleansed_emp_length")).\
        filter((col("cleansed_emp_length") != "n/a") & (col("cleansed_emp_length") < 10)).show()

