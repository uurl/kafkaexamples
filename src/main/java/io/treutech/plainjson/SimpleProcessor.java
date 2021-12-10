package io.treutech.plainjson;

import io.treutech.Constants;
import io.treutech.Person;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public final class SimpleProcessor {
  private final Logger logger = LogManager.getLogger(SimpleProducer.class);
  private final Consumer<String, String> consumer;
  private final Producer<String, String> producer;

  public SimpleProcessor(String brokers) {
    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", brokers);
    consumerProps.put("group.id", "person-processor");
    consumerProps.put("key.deserializer", StringDeserializer.class);
    consumerProps.put("value.deserializer", StringDeserializer.class);
    consumer = new KafkaConsumer<>(consumerProps);

    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", brokers);
    producerProps.put("key.serializer", StringSerializer.class);
    producerProps.put("value.serializer", StringSerializer.class);
    producer = new KafkaProducer<>(producerProps);
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void process() {
    consumer.subscribe(Collections.singletonList(Constants.getPersonsTopic()));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));

      for(ConsumerRecord<String, String> record : records) {
        final String personJson = record.value();
        logger.info("JSON data: " + personJson);
        Person person = null;
        try {
          person = Constants.getJsonMapper().readValue(personJson, Person.class);
        } catch (IOException e) {
          e.printStackTrace();
        }
        logger.info("Person: " + person);
        assert person != null;
        LocalDate birthDateLocal =
            person.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int age = Period.between(birthDateLocal, LocalDate.now()).getYears();
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(
            Constants.getAgesTopic(), person.firstName + ' ' + person.lastName, String.valueOf(age)));
        logger.info("Age: " + age);
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void main( String[] args) {
    new SimpleProcessor("localhost:9092").process();
  }
}
