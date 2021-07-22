package com.kafka.consumers

import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import java.util.Properties

import scala.collection.JavaConverters._

class KafkaConsumerApp(topicName: String, groupId: String, bootstrapServers: String) {

  def createConsumer(groupId: String, bootstrapServers: String): KafkaConsumer[String, String] = {
    val props: Properties = new Properties()
    props.put("group.id", groupId)
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    return consumer
  }

  def consumeFromKafka(topic: String, consumer: KafkaConsumer[String, String]): Unit = {
    if (topic != null) {
      println("Topic Name -> " + topic)
      try {
        consumer.subscribe(util.Arrays.asList(topicName))
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
        println("Records -> " + records.count())
        while (true) {
          for (record <- records.asScala) {
            println("Record -> " + record)
            readMessages(record)
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        consumer.close()
      }
    }
  }

  def readMessages(record: ConsumerRecord[String, String]) = {
    try {
      val topic = record.topic()
      val messageKey = record.key()
      val valueKey = record.value()
      val offsetId = record.offset()
      val partitionNo = record.partition()
      val message = createJsonMessage(topic, messageKey, valueKey, offsetId, partitionNo)
      println("Message -> " + message)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def createJsonMessage(topic: String, key: String, value: String, offsetId: Long, partitionId: Int): String = {
    val message = "{" + "TopicName : " + topic + "PartitionNo : " + partitionId + "OffsetID : " + offsetId +
      "Key : " + key + "Value : " + value
    return message
  }
}
