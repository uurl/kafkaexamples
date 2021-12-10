package io.treutech.withavro;

import com.github.javafaker.Faker;
import io.treutech.Constants;
import io.treutech.Person;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericData.Record;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public final class SimpleProducer {

  private final Producer<String, GenericRecord> producer;
  private Schema schema;

  public SimpleProducer(String brokers, String schemaRegistryUrl) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    producer = new KafkaProducer<>(props);
    try {
      schema = (new Parser()).parse(new File("src/main/resources/person.avsc"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void produce(int ratePerSecond) {
    long waitTimeBetweenIterationsMs = 1000L / (long)ratePerSecond;
    Faker faker = new Faker();

    while(true) {
      Person fakePerson = new Person(
          faker.name().firstName(),
          faker.name().lastName(),
          faker.date().birthday(),
          faker.address().city(),
          faker.internet().ipV4Address());
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
      recordBuilder.set("firstName", fakePerson.firstName);
      recordBuilder.set("lastName", fakePerson.lastName);
      recordBuilder.set("birthDate", fakePerson.birthDate.getTime());
      recordBuilder.set("city", fakePerson.city);
      recordBuilder.set("ipAddress", fakePerson.ipAddress);
      Record avroPerson = recordBuilder.build();
      Future<RecordMetadata> futureResult =
          producer.send(new ProducerRecord<>(Constants.getPersonsAvroTopic(), avroPerson));
      try {
        Thread.sleep(waitTimeBetweenIterationsMs);
        futureResult.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main( String[] args) {
    new SimpleProducer("localhost:9092", "http://localhost:8081").produce(2);
  }

}