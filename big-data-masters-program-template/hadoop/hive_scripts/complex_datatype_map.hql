CREATE  DATABASE IF NOT EXISTS npntraining_db;

CREATE TABLE npntraining_db.employeeMap(
    id INT,
    names array<String>,
    companyname STRING,
    language array<String>,
    salary BIGINT,
    deductions map<String,int>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$'
MAP KEYS TERMINATED BY '#';


LOAD DATA LOCAL INPATH '/home/${env:USER}/big-data-masters-program/hadoop/dataset/MapFile.csv'
overwrite into table npntraining_db.employeeMap;



-- select * from npntraining_db.employeeMap where deductions['Federal Taxes']>1000
