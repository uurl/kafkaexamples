package com.treutec.plainjson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.javafaker.Faker;
import com.treutec.Constants;
import com.treutec.Person;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class SimpleProducer {
  private final Producer<String, String> producer;

  public SimpleProducer(String brokers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", StringSerializer.class);
    props.put("value.serializer", StringSerializer.class);
    producer = new KafkaProducer<>(props);
  }

  public void produce(int ratePerSecond) {
    long waitTimeBetweenIterationsMs = 1000L / (long)ratePerSecond;
    Faker faker = new Faker();

    while(true) {
      Person fakePerson = new Person(
          faker.name().firstName(),
          faker.name().lastName(),
          faker.date().birthday(),
          faker.address().city(),
          faker.internet().ipV4Address()
      );
      String fakePersonJson = null;
      try {
        fakePersonJson = Constants.getJsonMapper().writeValueAsString(fakePerson);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      Future futureResult = producer.send(new ProducerRecord<>(Constants.getPersonsTopic(), fakePersonJson));
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
