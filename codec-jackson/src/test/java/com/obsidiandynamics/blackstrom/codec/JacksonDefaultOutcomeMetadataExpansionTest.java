package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.fasterxml.jackson.annotation.JsonInclude.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class JacksonDefaultOutcomeMetadataExpansionTest {
  @Test
  public void testSerializeDeserialize() throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Include.NON_NULL);
    final SimpleModule module = new SimpleModule();
    new JacksonDefaultOutcomeMetadataExpansion().accept(module);
    mapper.registerModule(module);
    
    final DefaultOutcomeMetadata meta = new DefaultOutcomeMetadata(100);
    final String encoded = mapper.writeValueAsString(meta);
    
    final DefaultOutcomeMetadata decoded = mapper.readValue(encoded, DefaultOutcomeMetadata.class);
    assertEquals(meta, decoded);
  }
}
