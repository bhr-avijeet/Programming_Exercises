SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

/* FROM npntraining_db.transaction_records txn INSERT into TABLE npntraining_db.transaction_records_partitioned
PARTITION(category)
select txn.txnno, txn.txndate,txn.custno,  
txn.amount,txn.product,txn.city,txn.state,txn.spendby,txn.category; */

-- show partitions transaction_records_partitioned;

/* FROM npntraining_db.transaction_records txn INSERT INTO TABLE npntraining_db.transaction_records_partitioned
PARTITION(category='Exercise & Fitness1')
select txn.txnno, txn.txndate,txn.custno,
txn.amount,txn.product,txn.city,txn.state,txn.spendby where category='Exercise & Fitness1'; */

--Dynamic Table Creation
create table npntraining.transaction_records _partitioned_bycat(txnno INT, txndate STRING, custno INT, amount DOUBLE,
product STRING, city STRING, state STRING, spendby STRING)
partitioned by (category STRING)
row format delimited fields terminated by ','
stored as textfile;

--Dynamic Load
/* FROM npntraining.transaction_records txn INSERT into TABLE npntraining.transaction_records_partitioned_bycat
PARTITION(category)
select txn.txnno, txn.txndate,txn.custno,  
txn.amount,txn.product,txn.city,txn.state,txn.spendby,txn.category DISTRIBUTE BY category; */
