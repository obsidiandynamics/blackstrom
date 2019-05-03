package com.obsidiandynamics.blackstrom.codec;

import com.fasterxml.jackson.databind.module.*;

public final class JacksonPayloadModule extends SimpleModule {
  private static final long serialVersionUID = 1L;
  
  public JacksonPayloadModule() {
    addSerializer(Payload.class, new JacksonPayloadSerializer());
    addDeserializer(Payload.class, new JacksonPayloadDeserializer());
  }
}
