package com.obsidiandynamics.blackstrom.codec;

import com.fasterxml.jackson.databind.module.*;

public final class JacksonVersionableModule extends SimpleModule {
  private static final long serialVersionUID = 1L;
  
  public JacksonVersionableModule() {
    addSerializer(Versionable.class, new JacksonVersionableSerializer());
    addDeserializer(Versionable.class, new JacksonVersionableDeserializer());
  }
}
