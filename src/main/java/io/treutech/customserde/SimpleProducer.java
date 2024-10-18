package io.treutech.customserde;

import io.treutech.Constants;
import io.treutech.Person;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class SimpleProducer {
  private final Logger log = LogManager.getLogger(SimpleProducer.class);

  private final Producer<String, Person> producer;

  public SimpleProducer(final String brokers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerializer.class);
    producer = new KafkaProducer<>(props);
  }

  public static void main(final String[] args) {
    new SimpleProducer("localhost:9092").produce(2);
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void produce(final int ratePerSecond) {
    final long waitTimeBetweenIterationsMs = 1000L / (long) ratePerSecond;
    final Faker faker = new Faker();

    while (true) {
      final Person fakePerson = new Person(
          faker.name().firstName(),
          faker.name().lastName(),
          faker.date().birthday(),
          faker.address().city(),
          faker.internet().ipV4Address()
      );
      log.debug("Produced Person: {}", fakePerson);
      final Future<RecordMetadata> futureResult =
          producer.send(new ProducerRecord<>(Constants.getPersonsTopic(), fakePerson));
      try {
        Thread.sleep(waitTimeBetweenIterationsMs);
        futureResult.get();
      } catch (final InterruptedException | ExecutionException e) {
        log.error(e.getMessage());
      }
    }
  }
}
