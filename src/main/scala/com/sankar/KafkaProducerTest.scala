package com.sankar

import java.util.HashMap

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaProducerTest extends App {

  // Zookeeper connection properties
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  // Send some messages
  while (true) {
    (1 to 2).foreach { messageNum =>
      val str = (1 to 5).map(x => scala.util.Random.nextInt(10).toString)
        .mkString(" ")

      val message = new ProducerRecord[String, String]("test", null, str)
      producer.send(message)
    }

    Thread.sleep(1000)
  }
}