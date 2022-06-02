@ECHO OFF
@RD /S /Q %KAFKA_HOME%\kafka-logs"
@RD /S /Q %KAFKA_HOME%\logs"
@RD /S /Q %KAFKA_HOME%\zookeeper-logs"
PAUSE
