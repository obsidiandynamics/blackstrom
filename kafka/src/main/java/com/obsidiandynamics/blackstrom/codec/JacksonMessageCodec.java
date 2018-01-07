package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.annotation.JsonInclude.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class JacksonMessageCodec implements MessageCodec {
  private final ObjectMapper mapper;
  
  public JacksonMessageCodec(boolean mapPayload) {
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Include.NON_NULL);
    
    final SimpleModule module = new SimpleModule();
    module.addSerializer(Message.class, new JacksonMessageSerializer());
    module.addDeserializer(Message.class, new JacksonMessageDeserializer(mapPayload));
    module.addSerializer(Payload.class, new JacksonPayloadSerializer());
    module.addDeserializer(Payload.class, new JacksonPayloadDeserializer());
    mapper.registerModule(module);
  }

  @Override
  public String encode(Message message) throws JsonProcessingException {
    return mapper.writeValueAsString(message);
  }

  @Override
  public Message decode(String text) throws JsonParseException, JsonMappingException, IOException {
    return mapper.readValue(text, Message.class);
  }
}
