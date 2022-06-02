CREATE  DATABASE IF NOT EXISTS npntraining_db;

CREATE TABLE npntraining_db.employee_array(
    id INT,
    names array<STRING>,
    companyName STRING,
    languages array<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$';

LOAD DATA LOCAL INPATH '/home/${env:USER}/big-data-masters-program/hadoop/dataset/ArrayFile.csv'
INTO TABLE npntraining_db.employee_array;


-- select names[0] from npntraining_db.employee_array;
-- select * from npntraining_db.employee_array where names[0]='naveen';