import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# catalog: database and table names, s3 output bucket
db_name = "glue-demo-db"
tbl_name = "coronainput"
s3_write_bucket = "s3://avijeet-bd-training/corona_data_analysis/output/"

############################
#        EXTRACT           #
############################

# creating datasource using the catalog table
datasource0 = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_name)

# converting from Glue DynamicFrame to Spark Dataframe
dataframe = datasource0.toDF()

############################
#        TRANSFORM         #
############################

# dropping the last update column
datasource_df = dataframe.drop('last update')

# dropping rows if a row contains more than 4 null values
corona_df = datasource_df.dropna(thresh=4)

# replacing the missing value in Province/State column and populating with a default value
cleansed_data_df = corona_df.fillna(
    value='na_province_state', subset='province/state')


# Grouping the records by Province/State and Country/Region column, aggregating with max(Confirmed) column
# and sorting them in descending order of max of Confirmed cases.

most_cases_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('confirmed')\
    .select('province/state', 'country/region', col("max(confirmed)").alias("Most_Cases"))\
    .orderBy('max(confirmed)', ascending=False)

# Grouping the records by Province/State and Country/Region column, aggregating with max(Deaths) column
# and sorting them in descending order of max of Deaths.

most_deaths_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('deaths')\
    .select('province/state', 'country/region', col("max(deaths)").alias("Most_Deaths"))\
    .orderBy('max(deaths)', ascending=False)

# Grouping the records by Province/State and Country/Region column, aggregating with max(Recovered) column
# and sorting them in descending order of max of Recovered.

most_recoveries_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('recovered')\
    .select('province/state', 'country/region', col("max(recovered)").alias("Most_Recovered"))\
    .orderBy('max(recovered)', ascending=False)
    
############################
#        LOAD              #
############################

most_cases_province_state_df.write.csv(path=s3_write_bucket+"/most-cases",mode="append")

job.commit()