package io.treutech.plainjson

import com.github.javafaker.Faker
import io.treutech.{Constants, Person}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager

import java.util.Properties

// ./bin/kafka-topics --create --topic persons --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

object JsonProducer {

  private final val logger = LogManager.getLogger(classOf[SimpleProducer])
  private final val brokers = "localhost:9092"
  private final val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  private final val producer = new KafkaProducer[String, String](props)

  def produce(ratePerSecond: Int): Unit = {
    val waitTimeBetweenIterationsMs = 1000L / ratePerSecond.toLong
    val faker = new Faker
    while (true) {
      val fakePerson = new Person(
        faker.name.firstName,
        faker.name.lastName,
        faker.date.birthday,
        faker.address.city,
        faker.internet.ipV4Address)
      logger.info("Produced Person: " + fakePerson)
      val fakePersonJson = Constants.getJsonMapper.writeValueAsString(fakePerson)
      val futureResult =
        producer.send(new ProducerRecord[String, String](Constants.getPersonsTopic, fakePersonJson))
      futureResult.get
      Thread.sleep(waitTimeBetweenIterationsMs)
    }
  }

  def main(args: Array[String]): Unit = produce(2)
}
