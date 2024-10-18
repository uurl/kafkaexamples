package io.treutech.withavro;

import io.treutech.Constants;
import io.treutech.Person;
import com.github.javafaker.Faker;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class SimpleProducer {
  private final Logger log = LogManager.getLogger(SimpleProducer.class);

  private final Producer<String, GenericRecord> producer;
  private Schema schema;

  public SimpleProducer(final String brokers, final String schemaRegistryUrl) {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    producer = new KafkaProducer<>(props);
    try {
      schema = (new Parser()).parse(new File("src/main/resources/person.avsc"));
    } catch (final IOException e) {
      log.error(e.getMessage());
    }
  }

  public static void main(final String[] args) {
    new SimpleProducer("localhost:9092", "http://localhost:8081").produce(2);
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
      final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
      recordBuilder.set("firstName", fakePerson.firstName);
      recordBuilder.set("lastName", fakePerson.lastName);
      recordBuilder.set("birthDate", fakePerson.birthDate.getTime());
      recordBuilder.set("city", fakePerson.city);
      recordBuilder.set("ipAddress", fakePerson.ipAddress);
      final Record avroPerson = recordBuilder.build();
      final Future<RecordMetadata> futureResult =
          producer.send(new ProducerRecord<>(Constants.getPersonsAvroTopic(), avroPerson));
      try {
        Thread.sleep(waitTimeBetweenIterationsMs);
        futureResult.get();
      } catch (final InterruptedException | ExecutionException e) {
        log.error(e.getMessage());
      }
    }
  }
}
