hive> ADD JAR /home/npntraining/JarFiles/hive-udf-0.0.1-SNAPSHOT.jar;
hive> create temporary function to_upper  as  'com.npntraining.udfs.hands.ToUpper';
