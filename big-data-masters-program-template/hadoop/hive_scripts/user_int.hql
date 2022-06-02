CREATE  DATABASE IF NOT EXISTS npntraining_db COMMENT 'Holds sample tables'
WITH DBPROPERTIES ('edited-by' = 'Naveen');

CREATE TABLE npntraining_db.users
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
tblproperties (
"skip.header.line.count"="1",
"skip.footer.line.count"="1"
              );


LOAD DATA LOCAL INPATH '/home/${env:USER}/big-data-masters-program/hadoop/dataset/users1.dat' INTO TABLE npntraining_db.users;
LOAD DATA LOCAL INPATH '/home/${env:USER}/big-data-masters-program/hadoop/dataset/users2.dat' INTO TABLE npntraining_db.users;
# LOAD DATA INPATH '/home/${env:USER}/big-data-masters-program/hadoop/dataset/users3.dat' INTO TABLE npntraining_db.users;



