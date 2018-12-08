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
    kryo.setRegistrationRequired(false);
    new KryoDefaultOutcomeMetadataExpansion().accept(kryo);
    
    final OutcomeMetadata meta = new OutcomeMetadata(100);
    final ByteBufferOutput out = new ByteBufferOutput(128, -1);
    kryo.writeObject(out, meta);
    
    final ByteBufferInput in = new ByteBufferInput(out.toBytes());
    final OutcomeMetadata decoded = kryo.readObject(in, OutcomeMetadata.class);
    
    assertEquals(meta, decoded);
  }
}
