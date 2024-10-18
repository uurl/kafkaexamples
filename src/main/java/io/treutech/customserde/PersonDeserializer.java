package io.treutech.customserde;

import io.treutech.Constants;
import io.treutech.Person;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public final class PersonDeserializer implements Deserializer<Person> {

  @Override
  public Person deserialize(final String topic, final byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      return Constants.getJsonMapper().readValue(data, Person.class);
    } catch (final IOException ioe) {
      return null;
    }
  }
}

