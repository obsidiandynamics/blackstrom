package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsontype.*;
import com.fasterxml.jackson.databind.ser.std.*;

class JacksonDogSerializer extends StdSerializer<Dog> {
  private static final long serialVersionUID = 1L;

  JacksonDogSerializer() {
    super(Dog.class);
  }

  @Override
  public void serializeWithType(Dog dog, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
    serialize(dog, gen, serializers);
  }

  @Override
  public void serialize(Dog dog, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("@type", "Dog");
    gen.writeStringField("name", dog.name);
    JacksonUtils.writeObject("friend", dog.friend, gen);
    gen.writeEndObject();
  }
}
