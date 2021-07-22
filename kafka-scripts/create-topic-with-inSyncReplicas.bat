@echo off
echo $$ Please enter the Following Inputs to create the Topic with inSync Replicas $$
echo ################################################################################
echo kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor %rf% --partitions %part% --topic %topic%  --config min.insync.replicas=%insync%

echo Please enter the TopicName ::
set /p topic="" 
echo Please enter the Replication Factor ::
set /p rf="" 
echo Please enter the No of Partitions ::
set /p part=""
echo Please enter the No of inSync Replicas ::
set /p insync=""
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor %rf% --partitions %part% --topic %topic%  --config min.insync.replicas=%insync%   >>  C:\Krishna\My_Learnings\KAFKA_HOME\TopicStatusLog.log
