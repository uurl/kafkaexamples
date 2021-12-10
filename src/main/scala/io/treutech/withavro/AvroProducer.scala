package io.treutech.withavro

import com.github.javafaker.Faker
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.treutech.{Constants, Person}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig

import java.io.File
import java.util.Properties

// ./bin/kafka-topics --create --topic avro-persons --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

object AvroProducer {
  private final val brokers = "localhost:9092"
  private final val schemaRegistryUrl = "http://localhost:8081"
  private final val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put("schema.registry.url", schemaRegistryUrl)
  private final val producer = new KafkaProducer[String, GenericRecord](props)
  private final val schema = (new Schema.Parser).parse(new File("src/main/resources/person.avsc"))

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
      val recordBuilder = new GenericRecordBuilder(schema)
      recordBuilder.set("firstName", fakePerson.firstName)
      recordBuilder.set("lastName", fakePerson.lastName)
      recordBuilder.set("birthDate", fakePerson.birthDate.getTime)
      recordBuilder.set("city", fakePerson.city)
      recordBuilder.set("ipAddress", fakePerson.ipAddress)
      val avroPerson = recordBuilder.build
      val futureResult =
        producer.send(new ProducerRecord[String, GenericRecord](
          Constants.getPersonsAvroTopic, avroPerson))
      Thread.sleep(waitTimeBetweenIterationsMs)
      futureResult.get
    }
  }

  def main(args: Array[String]): Unit = produce(2)
}
