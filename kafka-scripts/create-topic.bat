
@echo off
echo $$ Please enter the Following Inputs to create the Topic $$
echo ###########################################################
echo kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor %rf% --partitions %part% --topic %topic%

echo Please enter the TopicName ::
set /p topic=""
echo Please enter the Replication Factor ::
set /p rf=""
echo Please enter the No of Partitions ::
set /p part=""
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor %rf% --partitions %part% --topic %topic%
