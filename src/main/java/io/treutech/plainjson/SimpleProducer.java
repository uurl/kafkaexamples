package io.treutech.plainjson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.javafaker.Faker;
import io.treutech.Constants;
import io.treutech.Person;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public final class SimpleProducer {
  private final Logger logger = LogManager.getLogger(SimpleProducer.class);
  private final Producer<String, String> producer;

  public SimpleProducer(String brokers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", StringSerializer.class);
    props.put("value.serializer", StringSerializer.class);
    producer = new KafkaProducer<>(props);
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void produce(final int ratePerSecond) {
    long waitTimeBetweenIterationsMs = 1000L / (long) ratePerSecond;
    final Faker faker = new Faker();

    while (true) {
      Person fakePerson = new Person(
          faker.name().firstName(),
          faker.name().lastName(),
          faker.date().birthday(),
          faker.address().city(),
          faker.internet().ipV4Address()
      );
      logger.debug("Produced Person: " + fakePerson);
      String fakePersonJson = null;
      try {
        fakePersonJson = Constants.getJsonMapper().writeValueAsString(fakePerson);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      Future<RecordMetadata> futureResult =
          producer.send(new ProducerRecord<>(Constants.getPersonsTopic(), fakePersonJson));
      try {
        Thread.sleep(waitTimeBetweenIterationsMs);
        futureResult.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    new SimpleProducer("localhost:9092").produce(2);
  }
}
