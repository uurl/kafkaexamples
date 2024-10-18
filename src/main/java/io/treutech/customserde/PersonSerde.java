package io.treutech.customserde;

import io.treutech.Constants;
import io.treutech.Person;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public final class PersonSerde implements Serde<Person> {

  private static final Logger log = LogManager.getLogger(PersonSerde.class);

  @Override
  public Serializer<Person> serializer() {
    return (topic, data) -> {
      if (data == null) {
        log.warn("Attempting to serialize null data for topic: {}", topic);
        return null;
      }
      try {
        return Constants.getJsonMapper().writeValueAsBytes(data);
      } catch (final JsonProcessingException e) {
        log.error("Error serializing Person object for topic {}: {}", topic, e.getMessage());
        return null;
      }
    };
  }

  @Override
  public Deserializer<Person> deserializer() {
    return (topic, data) -> {
      if (data == null) {
        log.warn("Attempting to deserialize null data for topic: {}", topic);
        return null;
      }
      try {
        return Constants.getJsonMapper().readValue(data, Person.class);
      } catch (final IOException ioe) {
        log.error("Error deserializing data for topic {}: {}", topic, ioe.getMessage());
        return null;
      }
    };
  }
}
