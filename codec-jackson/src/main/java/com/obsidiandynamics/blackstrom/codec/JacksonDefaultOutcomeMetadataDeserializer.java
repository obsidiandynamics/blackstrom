package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class JacksonDefaultOutcomeMetadataDeserializer extends StdDeserializer<OutcomeMetadata> {
  private static final long serialVersionUID = 1L;
  
  JacksonDefaultOutcomeMetadataDeserializer() {
    super(OutcomeMetadata.class);
  }
  
  @Override
  public OutcomeMetadata deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    final JsonNode root = p.getCodec().readTree(p);
    final long proposalTimestamp = root.get("proposalTimestamp").asLong();
    return new OutcomeMetadata(proposalTimestamp);
  }
}
