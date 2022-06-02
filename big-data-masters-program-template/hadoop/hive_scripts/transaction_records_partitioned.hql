create table IF NOT EXISTS npntraining.transaction_records(
    txnno INT,
    txndate STRING,
    custno INT,
    amount DOUBLE,
    category STRING,
    product STRING,
    city STRING,
    state STRING,
    spendby STRING)
row format delimited
fields terminated by ','
stored as textfile;

load data local inpath '/home/${env:USER}/big-data-masters-program/hadoop/dataset/Transactions.txt'
into table npntraining_db.transaction_records;


CREATE TABLE IF NOT EXISTS npntraining.transaction_records_partitioned(
txnno INT, 
txndate STRING, 
custno INT, 
amount DOUBLE,
product STRING, 
city STRING, 
state STRING, 
spendby STRING)
partitioned by (category STRING)
row format delimited fields terminated by ','
stored as textfile;

INSERT OVERWRITE TABLE npntraining.transaction_records_partitioned
PARTITION(category='Exercise & Fitness')
select txn.txnno, txn.txndate,txn.custno,  
txn.amount,txn.product,txn.city,txn.state,txn.spendby FROM npntraining.transaction_records txn 
where category='Exercise & Fitness';

 
FROM npntraining_db.transaction_records txn INSERT INTO TABLE npntraining_db.transaction_records_partitioned
PARTITION(category='Gymnastics')
select txn.txnno, txn.txndate,txn.custno,
txn.amount,txn.product,txn.city,txn.state,txn.spendby where category='Gymnastics';


-- show partitions transaction_records_partitioned;


-- select count(distinct(category)) from transaction_records;

