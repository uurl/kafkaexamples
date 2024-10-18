package io.treutech.withavro;

import io.treutech.Constants;
import io.treutech.Person;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class SimpleProcessor {
  private final Logger log = LogManager.getLogger(SimpleProcessor.class);
  private final Consumer<String, GenericRecord> consumer;
  private final Producer<String, String> producer;

  public SimpleProcessor(final String brokers, final String schemaRegistryUrl) {
    final Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "person-processor");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    consumerProps.put("schema.registry.url", schemaRegistryUrl);
    consumer = new KafkaConsumer<>(consumerProps);

    final Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer = new KafkaProducer<>(producerProps);
  }

  public static void main(final String[] args) {
    new SimpleProcessor("localhost:9092", "http://localhost:8081").process();
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void process() {
    consumer.subscribe(Collections.singletonList(Constants.getPersonsAvroTopic()));

    while (true) {
      final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1L));

      for (final ConsumerRecord<String, GenericRecord> record : records) {
        final GenericRecord personAvro = record.value();
        final Person person = new Person(personAvro.get("firstName").toString(),
            personAvro.get("lastName").toString(),
            new Date((Long) personAvro.get("birthDate")));
        final LocalDate startDate = person.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        final int age = Period.between(startDate, LocalDate.now()).getYears();
        final Future<RecordMetadata> future = producer.send(new ProducerRecord<>(
            Constants.getAgesTopic(), person.firstName + ' ' + person.lastName, String.valueOf(age)));
        try {
          future.get();
        } catch (final InterruptedException | ExecutionException e) {
          log.error(e.getMessage());
        }
      }
    }
  }
}
