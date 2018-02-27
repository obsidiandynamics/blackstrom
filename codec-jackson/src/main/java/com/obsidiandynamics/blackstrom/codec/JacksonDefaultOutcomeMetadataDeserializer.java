package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class JacksonDefaultOutcomeMetadataDeserializer extends StdDeserializer<DefaultOutcomeMetadata> {
  private static final long serialVersionUID = 1L;
  
  JacksonDefaultOutcomeMetadataDeserializer() {
    super(DefaultOutcomeMetadata.class);
  }
  
  @Override
  public DefaultOutcomeMetadata deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    final JsonNode root = p.getCodec().readTree(p);
    final long proposalTimestamp = root.get("proposalTimestamp").asLong();
    return new DefaultOutcomeMetadata(proposalTimestamp);
  }
}
