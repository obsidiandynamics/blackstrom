package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;

final class JacksonNilDeserializer extends StdDeserializer<Nil> {
  private static final long serialVersionUID = 1L;
  
  JacksonNilDeserializer() {
    super(Nil.class);
  }
  
  @Override
  public Nil deserialize(JsonParser p, DeserializationContext ctxt) {
    return Nil.getInstance();
  }
}
