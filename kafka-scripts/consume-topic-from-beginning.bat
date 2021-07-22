@echo off
echo $$ Please provide the TopicName for Consumption    --from-beginning $$
echo #######################################################################
echo %KAFKA_HOME%\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic %topic% --from-beginning

echo Please enter the TopicName ::
set /p topic=""
%KAFKA_HOME%\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic %topic% --from-beginning


