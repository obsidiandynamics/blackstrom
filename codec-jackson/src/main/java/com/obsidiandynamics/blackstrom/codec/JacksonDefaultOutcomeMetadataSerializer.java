package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public class JacksonDefaultOutcomeMetadataSerializer extends StdSerializer<DefaultOutcomeMetadata> {
  private static final long serialVersionUID = 1L;

  JacksonDefaultOutcomeMetadataSerializer() {
    super(DefaultOutcomeMetadata.class);
  }

  @Override
  public void serialize(DefaultOutcomeMetadata metadata, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeNumberField("proposalTimestamp", metadata.getProposalTimestamp());
    gen.writeEndObject();
  }
}
