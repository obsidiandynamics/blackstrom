package com.obsidiandynamics.blackstrom.codec;

import com.fasterxml.jackson.databind.module.*;

public final class JacksonVariantModule extends SimpleModule {
  private static final long serialVersionUID = 1L;
  
  public JacksonVariantModule() {
    addSerializer(UniVariant.class, new JacksonVariantSerializer());
    addDeserializer(UniVariant.class, new JacksonVariantDeserializer());
  }
}
