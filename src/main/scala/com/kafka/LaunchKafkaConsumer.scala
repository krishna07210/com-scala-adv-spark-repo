package com.kafka

import com.kafka.consumers.KafkaConsumerApp
import org.apache.kafka.clients.consumer.KafkaConsumer

object LaunchKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val topicName = "InvoiceTopic"
    val consumerGroup = "invoice-pumping-group1"
    val bootstrapServers = "localhost:9092"
    val kafkaConsumerObj = new KafkaConsumerApp(topicName, consumerGroup, bootstrapServers)
    val kafkaConsumer: KafkaConsumer[String, String] = kafkaConsumerObj.createConsumer(consumerGroup, bootstrapServers)
    println("kafkaConsumer -> " + kafkaConsumer)
    kafkaConsumerObj.consumeFromKafka(topicName, kafkaConsumer)
  }
}
