package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsontype.*;
import com.fasterxml.jackson.databind.ser.std.*;

class JacksonDogSerializer extends StdSerializer<JacksonDog> {
  private static final long serialVersionUID = 1L;

  JacksonDogSerializer() {
    super(JacksonDog.class);
  }

  @Override
  public void serializeWithType(JacksonDog dog, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
    serialize(dog, gen, serializers);
  }

  @Override
  public void serialize(JacksonDog dog, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("@type", "Dog");
    gen.writeStringField("name", dog.name);
    JacksonUtils.writeObject("friend", dog.friend, gen);
    gen.writeEndObject();
  }
}
