package com.obsidiandynamics.blackstrom.codec;

import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.codec.JacksonMessageCodec.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class JacksonDefaultOutcomeMetadataExpansion implements JacksonExpansion {
  @Override
  public void accept(SimpleModule module) {
    module.addSerializer(OutcomeMetadata.class, new JacksonDefaultOutcomeMetadataSerializer());
    module.addDeserializer(OutcomeMetadata.class, new JacksonDefaultOutcomeMetadataDeserializer());
  }
}
