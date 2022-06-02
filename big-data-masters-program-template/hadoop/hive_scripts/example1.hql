CREATE TABLE npntraining.example1(
id INT,
name STRING,
mobile_no STRING,
hobbies ARRAY<STRING>,
employment_type STRING,
income BIGINT,
identification MAP<STRING,STRING>,
dob STRING,
address STRUCT<state:STRING,city:STRING,pincode:BIGINT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$'
MAP KEYS TERMINATED BY '#'
LOCATION '/external_dir'
tblproperties(
"skip.header.line.count"="1",
"skip.footer.line.count"="1"
);

--LOAD DATA LOCAL INPATH '${env:DATA_SET}/example1.csv' OVERWRITE INTO TABLE example1;

--hdfs dfs -rm /external_dir/example1.csv

--hdfs dfs -put $DATA_SET/example1.csv /external_dir/