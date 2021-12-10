package io.treutech.customserde

import io.treutech.{Constants, Person}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.{Duration, LocalDate, Period, ZoneId}
import java.util.{Collections, Properties}

object CustomProcessor {
  private final val brokers = "localhost:9092"

  private final val consumerProps = new Properties
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "person-processor")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[PersonDeserializer])
  private final val consumer = new KafkaConsumer[String, Person](consumerProps)

  private final val producerProps = new Properties
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  private final val producer = new KafkaProducer[String, String](producerProps)

  def process(pollDuration: Int): Unit = {
    consumer.subscribe(Collections.singletonList(Constants.getPersonsTopic))
    while (true) {
      val records = consumer.poll(Duration.ofSeconds(pollDuration))
      records.forEach( r => {
        val person = r.value
        val birthDateLocal = person.birthDate.toInstant.atZone(ZoneId.systemDefault).toLocalDate
        val age = Period.between(birthDateLocal, LocalDate.now).getYears
        val future = producer.send(new ProducerRecord[String, String](
          Constants.getAgesTopic, person.firstName + ' ' + person.lastName, String.valueOf(age)))
        future.get
      })
    }
  }

  def main(args: Array[String]): Unit = process(1)

}