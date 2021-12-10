package io.treutech.customserde;

import com.github.javafaker.Faker;
import io.treutech.Constants;
import io.treutech.Person;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public final class SimpleProducer {
  private final Producer<String, Person> producer;

  public SimpleProducer(String brokers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerializer.class);
    producer = new KafkaProducer<>(props);
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void produce(int ratePerSecond) {
    long waitTimeBetweenIterationsMs = 1000L / (long) ratePerSecond;
    Faker faker = new Faker();

    while (true) {
      Person fakePerson = new Person(
          faker.name().firstName(),
          faker.name().lastName(),
          faker.date().birthday(),
          faker.address().city(),
          faker.internet().ipV4Address());
      Future<RecordMetadata> futureResult =
          producer.send(new ProducerRecord<>(Constants.getPersonsTopic(), fakePerson));
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
