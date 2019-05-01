package com.obsidiandynamics.blackstrom.codec;

import java.io.*;
import java.util.function.*;

import com.fasterxml.jackson.annotation.JsonInclude.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.yconf.*;

@Y
public class JacksonMessageCodec implements MessageCodec {  
  public static final int ENCODING_VERSION = 2;
  
  private static final JacksonExpansion[] DEF_EXPANSIONS = { new JacksonDefaultOutcomeMetadataExpansion() };
  
  @FunctionalInterface
  public interface JacksonExpansion extends Consumer<SimpleModule> {}

  private final ObjectMapper mapper;
  
  private static ObjectMapper createDefaultBaseMapper() {
    return new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, true)
        .setSerializationInclusion(Include.NON_NULL);
  }
  
  public JacksonMessageCodec(@YInject(name="mapPayload") boolean mapPayload, 
                             @YInject(name="expansions") JacksonExpansion... expansions) {
    this(createDefaultBaseMapper(), mapPayload, expansions);
  }
  
  public JacksonMessageCodec(ObjectMapper mapper,
                             boolean mapPayload, 
                             JacksonExpansion... expansions) {
    this.mapper = mapper;
    
    final var module = new SimpleModule();
    module.addSerializer(Message.class, new JacksonMessageSerializer());
    module.addDeserializer(Message.class, new JacksonMessageDeserializer(mapPayload));
    module.addSerializer(Payload.class, new JacksonPayloadSerializer());
    module.addDeserializer(Payload.class, new JacksonPayloadDeserializer());
    
    for (var expansion : DEF_EXPANSIONS) expansion.accept(module);
    for (var expansion : expansions) expansion.accept(module);
    
    mapper.registerModule(module);
  }
  
  /**
   *  Obtains the underlying object mapper.
   *  
   *  @return The backing {@link ObjectMapper} instance.
   */
  public ObjectMapper getMapper() {
    return mapper;
  }
  
  @Override
  public byte[] encode(Message message) throws JsonProcessingException {
    return mapper.writeValueAsBytes(message);
  }

  @Override
  public Message decode(byte[] bytes) throws JsonParseException, JsonMappingException, IOException {
    return mapper.readValue(bytes, Message.class);
  }
}
