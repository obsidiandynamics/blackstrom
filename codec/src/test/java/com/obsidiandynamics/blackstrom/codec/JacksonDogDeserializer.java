package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;

final class JacksonDogDeserializer extends StdDeserializer<Dog> {
  private static final long serialVersionUID = 1L;
  
  JacksonDogDeserializer() {
    super(Dog.class);
  }
  
  @Override
  public Dog deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    final JsonNode root = p.getCodec().readTree(p);
    final String name = root.get("name").asText();
    
    final Animal<?> friend = JacksonUtils.readObject("friend", root, p, Animal.class);
    return new Dog().named(name).withFriend(friend);
  }
}
