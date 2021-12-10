package io.treutech.customserde

import com.github.javafaker.Faker
import io.treutech.{Constants, Person}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object CustomProducer {

  private final val brokers = "localhost:9092"
  private final val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[PersonSerializer])
  private final val producer = new KafkaProducer[String, Person](props)

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
      val futureResult = producer.send(new ProducerRecord[String, Person](Constants.getPersonsTopic, fakePerson))
      Thread.sleep(waitTimeBetweenIterationsMs)
      futureResult.get
    }
  }

  def main(args: Array[String]): Unit = produce(2)
}

