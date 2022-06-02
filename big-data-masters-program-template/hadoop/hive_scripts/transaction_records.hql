create table transaction_records(txnno INT, txndate STRING, custno INT, amount DOUBLE, 
category STRING, product STRING, city STRING, state STRING, spendby STRING)
row format delimited
fields terminated by ','
stored as textfile;


--LOAD DATA LOCAL INPATH '${env:DATA_SET}/Transactions.txt' OVERWRITE INTO TABLE transaction_records;


