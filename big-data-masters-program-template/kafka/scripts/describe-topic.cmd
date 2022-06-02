set /p topic=Enter topic name?
%KAFKA_HOME%\bin\windows\kafka-topics.bat --describe  --zookeeper localhost:2181 --topic %topic%