@echo off
echo $$ Please provide the TopicName for Produce the Messages    $$
echo ##############################################################
echo kafka-console-producer.bat --broker-list localhost:9092 --topic %topic% --property parse.key=true --property key.separator=":"

echo Please enter the TopicName ::
set /p topic=""

%KAFKA_HOME%\bin\windows\kafka-console-producer.bat --broker-list localhost:9092,localhost:9093 --topic %topic%  --property parse.key=true --property key.separator=":"
