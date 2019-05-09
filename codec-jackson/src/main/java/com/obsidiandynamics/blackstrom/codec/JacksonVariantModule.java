package com.obsidiandynamics.blackstrom.codec;

import com.fasterxml.jackson.databind.module.*;

public final class JacksonVariantModule extends SimpleModule {
  private static final long serialVersionUID = 1L;
  
  public JacksonVariantModule() {
    addSerializer(UniVariant.class, new JacksonUniVariantSerializer());
    addDeserializer(UniVariant.class, new JacksonUniVariantDeserializer());
    addSerializer(MultiVariant.class, new JacksonMultiVariantSerializer());
    addDeserializer(MultiVariant.class, new JacksonMultiVariantDeserializer());
    addDeserializer(Variant.class, new JacksonVariantDeserializer());
    addSerializer(Nil.class, new JacksonNilSerializer());
    addDeserializer(Nil.class, new JacksonNilDeserializer());
  }
}
