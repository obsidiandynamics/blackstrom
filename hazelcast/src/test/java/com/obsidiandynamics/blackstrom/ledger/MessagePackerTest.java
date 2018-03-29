package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class MessagePackerTest {
  @Test
  public void testConformance() throws Exception {
    Assertions.assertUtilityClassWellDefined(MessagePacker.class);
  }

  @Test
  public void testPackUnpackWithoutShardKey() throws Exception {
    final MessageCodec codec = new IdentityMessageCodec();
    final Message m = new Proposal("100", new String[0], null, 0).withShard(0);
    final byte[] encoded = MessagePacker.pack(codec, m);
    assertNotNull(encoded);
    
    final Message decoded = MessagePacker.unpack(codec, encoded);
    assertEquals(m, decoded);
  }

  @Test
  public void testPackUnpackWithShardKey() throws Exception {
    final MessageCodec codec = new IdentityMessageCodec();
    final Message m = new Proposal("100", new String[0], null, 0).withShardKey("shardKey").withShard(0);
    final byte[] encoded = MessagePacker.pack(codec, m);
    assertNotNull(encoded);

    final Message decoded = MessagePacker.unpack(codec, encoded);
    assertEquals(m, decoded);
  }

  @Test(expected=MessagePacker.DeserializationException.class)
  public void testUnpackWithRemainingBytes() throws Exception {
    final MessageCodec codec = new IdentityMessageCodec();
    final Message m = new Proposal("100", new String[0], null, 0);
    final byte[] encoded = MessagePacker.pack(codec, m);
    assertNotNull(encoded);
    
    final byte[] padded = new byte[encoded.length + 1];
    System.arraycopy(encoded, 0, padded, 0, encoded.length);
    MessagePacker.unpack(codec, padded);
  }
}
