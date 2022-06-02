CREATE  DATABASE IF NOT EXISTS npntraining_db;
CREATE TABLE npntraining_db.employeeStruct(
    id INT,
    names array<String>,
    companyname STRING,
    language array<String>,
    salary BIGINT,
    deductions map<String,int>,
    address struct<state:STRING,city:STRING,pincode:BIGINT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$'
MAP KEYS TERMINATED BY '#';



LOAD DATA LOCAL INPATH '/home/${env:USER}/big-data-masters-program/hadoop/dataset/StructFile.csv'
overwrite into table npntraining_db.employeeStruct;

-- SELECT address.pincode from npntraining_db.employeeStruct;