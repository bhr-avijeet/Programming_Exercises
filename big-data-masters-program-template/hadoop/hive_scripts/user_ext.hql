CREATE  DATABASE IF NOT EXISTS npntraining_db COMMENT 'Holds sample tables'
WITH DBPROPERTIES ('edited-by' = 'Naveen');

CREATE EXTERNAL TABLE npntraining_db.users_ext
(
    user_id INT COMMENT 'user Id',
    first_name STRING COMMENT 'user name',
    last_name STRING,
    gender STRING,
    designation STRING,
    city STRING,
    country STRING,
    dob DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
Stored as TextFile
LOCATION '/external_dir'
tblproperties (
"skip.header.line.count"="1",
"skip.footer.line.count"="1"
              );

# hdfs dfs -put /home/npntraining/big-data-masters-program/hadoop/dataset/users1.dat /external_dir


#!hdfs dfs -put /home/npntraining/big-data-masters-program/hadoop/dataset/users1.dat /;
#LOAD DATA INPATH '/users3.dat' INTO TABLE npntraining_db.users;

