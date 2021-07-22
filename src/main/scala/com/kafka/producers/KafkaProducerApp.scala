package com.kafka.producers

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerApp(topicName: String, bootstrapServer: String, clientId: String) {

  def createProducer(clientId: String, bootstrapServers: String): KafkaProducer[String, String] = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    return producer
  }

  def pumpMessages(producer: KafkaProducer[String, String], topic: String, noOfMessage: Int): Unit = {
    try {
      for (i <- 0 to 15) {
        val message = createJsonMessage(i)
        val record = new ProducerRecord[String, String](topic, i.toString, message)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
  }

  def createJsonMessage(msgNo: Int): String = {
    val message =
      "{ \"InvoiceNumber\":" + 78302645 + msgNo + ",\"StoreID\":\"STR8513\",\"PosID\":\"POS155\",\"CustomerType\":\"NONPRIME\",\"TotalAmoun\":" + (6543.0 + msgNo) + ",\"NumberOfItems\":" + (msgNo + 1) + ",\"TaxableAmount\":" + (6543.0 + msgNo) + ",\"CGST\":" + (163.57500000000002 + msgNo) + ",\"SGST\":" + (163.57500000000002 + msgNo) + ",\"CESS\":" + (8.17875 + msgNo) + ",\"DeliveryType\":\"HOME-DELIVERY\"}"
    return message
  }
}
