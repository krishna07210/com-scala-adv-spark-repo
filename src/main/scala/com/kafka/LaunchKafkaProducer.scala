package com.kafka

import com.kafka.producers.KafkaProducerApp
import org.apache.kafka.clients.producer.KafkaProducer

object LaunchKafkaProducer {
  def main(args: Array[String]): Unit = {
    val topicName = "invoices2"
    val clientId = "invoice-01"
    val bootstrapServers = "localhost:9092"
    val kafkaProducerObj = new KafkaProducerApp(topicName, bootstrapServers, clientId)
    val kafkaProducer: KafkaProducer[String, String] = kafkaProducerObj.createProducer(clientId, bootstrapServers)
    kafkaProducerObj.pumpMessages(kafkaProducer, topicName, 10)
  }
}
