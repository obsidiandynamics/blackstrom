package com.obsidiandynamics.blackstrom.codec;

import com.fasterxml.jackson.databind.module.*;

public final class JacksonVariantModule extends SimpleModule {
  private static final long serialVersionUID = 1L;
  
  public JacksonVariantModule() {
    addSerializer(MonoVariant.class, new JacksonMonoVariantSerializer());
    addDeserializer(MonoVariant.class, new JacksonMonoVariantDeserializer());
    addSerializer(PolyVariant.class, new JacksonPolyVariantSerializer());
    addDeserializer(PolyVariant.class, new JacksonPolyVariantDeserializer());
    addDeserializer(Variant.class, new JacksonVariantDeserializer());
    addSerializer(Nil.class, new JacksonNilSerializer());
    addDeserializer(Nil.class, new JacksonNilDeserializer());
  }
}
