package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;

final class JacksonCatDeserializer extends StdDeserializer<JacksonCat> {
  private static final long serialVersionUID = 1L;
  
  JacksonCatDeserializer() {
    super(JacksonCat.class);
  }
  
  @Override
  public JacksonCat deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    final JsonNode root = p.getCodec().readTree(p);
    final String name = root.get("name").asText();
    
    final JacksonAnimal<?> friend = JacksonUtils.readObject("friend", root, p, JacksonAnimal.class);
    return new JacksonCat().named(name).withFriend(friend);
  }
}
