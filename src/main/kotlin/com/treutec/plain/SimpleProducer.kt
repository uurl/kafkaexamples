package com.treutec.plain

import com.github.javafaker.Faker
import com.treutec.Person
import com.treutec.jsonMapper
import com.treutec.personsTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.util.Properties

// kafka-topics --zookeeper localhost:2181 --create --topic persons --replication-factor 1 --partitions 4
// kafka-console-consumer --bootstrap-server localhost:9092 --topic persons --property print.key=true

fun main(args: Array<String>) {
    SimpleProducer("localhost:9092").produce(2)
}

class SimpleProducer(brokers: String) {

    private val logger = LogManager.getLogger(javaClass)
    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }

    fun produce(ratePerSecond: Int) {
        val waitTimeBetweenIterationsMs = 1000L / ratePerSecond
        logger.info("Producing $ratePerSecond records per second (1 every ${waitTimeBetweenIterationsMs}ms)")

        val faker = Faker()
        while (true) {
            val fakePerson = Person(
                    firstName = faker.name().firstName(),
                    lastName = faker.name().lastName(),
                    birthDate = faker.date().birthday(),
                    city = faker.address().city(),
                    ipAddress = faker.internet().ipV4Address()
            )
            logger.info("Generated a person: $fakePerson")

            val fakePersonJson = jsonMapper.writeValueAsString(fakePerson)
            logger.debug("JSON data: $fakePersonJson")

            val futureResult = producer.send(ProducerRecord(personsTopic, fakePersonJson))
            logger.debug("Sent a record")

            Thread.sleep(waitTimeBetweenIterationsMs)

            // wait for the write acknowledgment
            futureResult.get()
        }
    }
}
