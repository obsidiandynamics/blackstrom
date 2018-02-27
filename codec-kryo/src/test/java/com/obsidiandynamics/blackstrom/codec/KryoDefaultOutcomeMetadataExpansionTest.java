package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import org.junit.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class KryoDefaultOutcomeMetadataExpansionTest {
  @Test
  public void testSerializeDeserialize() {
    final Kryo kryo = new Kryo();
    kryo.setReferences(false);
    new KryoDefaultOutcomeMetadataExpansion().accept(kryo);
    
    final DefaultOutcomeMetadata meta = new DefaultOutcomeMetadata(100);
    final ByteBufferOutput out = new ByteBufferOutput(128, -1);
    kryo.writeObject(out, meta);
    
    final ByteBufferInput in = new ByteBufferInput(out.toBytes());
    final DefaultOutcomeMetadata decoded = kryo.readObject(in, DefaultOutcomeMetadata.class);
    
    assertEquals(meta, decoded);
  }
}
