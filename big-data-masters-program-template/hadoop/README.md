
# Hadoop Eco-system

- [Hadoop Eco-system](#hadoop-eco-system)
  - [Hadoop](#hadoop)
    - [Hadoop UI](#hadoop-ui)
    - [Starting Hadoop Services](#starting-hadoop-services)
    - [Troubleshooting](#troubleshooting)
    - [Executing MapReduce Program](#executing-mapreduce-program)
  - [Hive](#hive)
    - [Commands to Start Hive Services](#commands-to-start-hive-services)
    - [Executing script from Hive terminal](#executing-script-from-hive-terminal)
    - [Executing script from shell](#executing-script-from-shell)
    - [JDBC]()
    - [UDF](#udf)
## Hadoop


### Hadoop UI

**NameNode** : http://localhost:8088/cluster/

**Resource Manager UI** : http://localhost:50070/dfshealth.html#tab-overview

### Starting Hadoop Services

```
[naveen@npntraining ~]$ sudo service ssh start
[naveen@npntraining ~]$ start-dfs.sh
[npntraining@centos8 ~]$ jps
688 SecondaryNameNode
471 DataNode
285 NameNode
[naveen@npntraining ~]$ start-yarn.sh
[npntraining@centos8 ~]$ jps
688 SecondaryNameNode
978 NodeManager
835 ResourceManager
471 DataNode
285 NameNode
[naveen@npntraining ~]$ mr-jobhistory-daemon.sh start historyserver
```

### Troubleshooting
If any of the service doesn't start follow the steps


```
[naveen@npntraining ~]$ cd $HADOOPO_HOME/dfsdata
[naveen@npntraining ~]$ rm -r *
[naveen@npntraining ~]$ hdfs namenode -format
[naveen@npntraining ~]$ start-dfs.sh
[naveen@npntraining ~]$ start-yarn.sh
```

### Executing MapReduce Program


```
[naveen@npntraining ~]$ cd $HADOOP_HOME/share/hadoop/mapreduce
[naveen@npntraining ~]$ yarn jar hadoop-mapreduce-examples-2.10.1.jar wordcount /words.txt /output_01
```

**View the output file**

```
[naveen@npntraining ~]$ hdfs dfs -cat /output_01/part*
```


## Hive

### Commands to Start Hive Services

```
[npntraining@centos8 ~]$ sudo service mysql start
[npntraining@centos8 ~]$ hive --service metastore &
```

### Executing script from Hive terminal

```
hive> source /home/npntraining/big-data-masters-program/employee.hql
```

### Executing script from shell

```
[npntraining@centos8 ~]$ hive -f employee.hql
```

### Command to Start Hive Server

```
hive --service hiveserver2 &
```

### UDF

```
hive> ADD JAR /home/npntraining/hive-udf-0.0.1-SNAPSHOT.jar;
hive> create temporary function to_upper  as  'com.npntraining.udfs.hands.ToUpper';
```


## Connecting to Hive via Python

pip install thrift

pip install thrift-sasl

pip install PyHive

sudo apt install libsasl2-dev

pip install sasl
