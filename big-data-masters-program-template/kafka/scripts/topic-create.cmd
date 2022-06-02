set /p topic=Enter topic name?
set /p partitions=Enter number of partitions?
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --partitions %partitions% --replication-factor 1 --topic %topic%