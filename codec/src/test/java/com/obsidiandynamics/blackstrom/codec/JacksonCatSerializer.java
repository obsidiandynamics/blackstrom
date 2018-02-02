package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsontype.*;
import com.fasterxml.jackson.databind.ser.std.*;

class JacksonCatSerializer extends StdSerializer<Cat> {
  private static final long serialVersionUID = 1L;

  JacksonCatSerializer() {
    super(Cat.class);
  }
  
  @Override
  public void serializeWithType(Cat cat, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
    serialize(cat, gen, serializers);
  }

  @Override
  public void serialize(Cat cat, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("@type", "Cat");
    gen.writeStringField("name", cat.name);
    JacksonUtils.writeObject("friend", cat.friend, gen);
    gen.writeEndObject();
  }
}
