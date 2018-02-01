package com.obsidiandynamics.blackstrom.codec.kryo;

import static org.junit.Assert.*;

import org.hamcrest.core.*;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.codec.kryo.KryoMessageSerializer.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class KryoMessageCodecTest implements TestSupport {
  private static void logEncoded(byte[] encoded) {
    if (LOG) LOG_STREAM.format("encoded:\n%s\n", BinaryUtils.dump(encoded));
  }
  
  private static void logReencoded(byte[] reencoded) {
    if (LOG) LOG_STREAM.format("re-encoded:\n%s\n", BinaryUtils.dump(reencoded));
  }
  
  private static void logDecoded(Message m, Object p) {
    if (LOG) LOG_STREAM.format("decoded %s (type=%s)\n", m, (p != null ? p.getClass().getSimpleName() : "n/a"));
  }
  
  @Rule 
  public ExpectedException thrown = ExpectedException.none();
  
  @Test
  public void testProposalNullObjective() throws Exception {
    final Message m = new Proposal("N100", new String[] {"a", "b"}, null, 1000).withSource("test");
    MessageCodec c;
    
    c = new KryoMessageCodec(false);
    final byte[] encoded = c.encode(m);
    logEncoded(encoded);

    final Proposal d1 = (Proposal) c.decode(encoded);
    logDecoded(d1, d1.getObjective());
    assertEquals(m, d1);
    
    final byte[] reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertArrayEquals(encoded, reencoded);

    c = new KryoMessageCodec(true);
    final Proposal d2 = (Proposal) c.decode(reencoded);
    logDecoded(d2, d2.getObjective());
    assertEquals(m, d2);
  }
  
  @Test
  public void testCycle() throws Exception {
    testCycle(1_000);
  }
  
  @Test
  public void testCycleBenchmark() throws Exception {
    if (TestBenchmark.isEnabled(KryoMessageCodecTest.class)) {
      System.out.println("Starting benchmark");
      testCycle(20_000_000);
    }
  }
  
  private static void testCycle(int runs) throws Exception {
    final MessageCodec c = new KryoMessageCodec(false);
    cycle(runs, 
          c, 
          new Proposal("N100", new String[] {"a", "b"}, null, 1000), 
          "hollow");
    cycle(runs, 
          c, 
          new Proposal("N100", new String[] {"a", "b"}, new Dog().named("Rex").withFriend(new Cat().named("Tigger")), 1000), 
          "filled");
  }
  
  private static void cycle(int runs, MessageCodec c, Message m, String name) throws Exception {
    final long took = TestSupport.tookThrowing(() -> {
      for (int i = 0; i < runs; i++) {
        final byte[] encoded = c.encode(m);
        c.decode(encoded);
      }
    });
    
    System.out.format("%s: %,d took %,d ms, %,.0f msgs/sec\n", 
                      name, runs, took, (double) runs / took * 1000);
  }
  
  @Test
  public void testProposalNonNullObjective() throws Exception {
    final Animal<?> a = new Dog().named("Rover").withFriend(new Cat().named("Misty"));
    final Proposal m = new Proposal("N100", new String[] {"a", "b"}, a, 1000);
    MessageCodec c;

    c = new KryoMessageCodec(false);
    final byte[] encoded = c.encode(m);
    logEncoded(encoded);
    
    final Proposal d1 = (Proposal) c.decode(encoded);
    logDecoded(d1, d1.getObjective());
    assertNotNull(d1.getObjective());
    assertEquals(PayloadBuffer.class, d1.getObjective().getClass());
    
    final byte[] reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertArrayEquals(encoded, reencoded);
    
    c = new KryoMessageCodec(true);
    final Proposal d2 = (Proposal) c.decode(reencoded);
    logDecoded(d2, d2.getObjective());
    assertEquals(m, d2);
  }

  @Test
  public void testVoteNonNullMetadata() throws Exception {
    final Animal<?> a = new Dog().named("Rex").withFriend(new Cat().named("Tigger"));
    final Vote m = new Vote("V100", new Response("test-cohort", Intent.ACCEPT, a)).withSource("test");
    MessageCodec c;

    c = new KryoMessageCodec(false);
    final byte[] encoded = c.encode(m);
    logEncoded(encoded);
    
    final Vote d1 = (Vote) c.decode(encoded);
    logDecoded(d1, d1.getResponse().getMetadata());
    assertNotNull(d1.getResponse().getMetadata());
    assertEquals(PayloadBuffer.class, d1.getResponse().getMetadata().getClass());
    
    final byte[] reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertArrayEquals(encoded, reencoded);
    
    c = new KryoMessageCodec(true);
    final Vote d2 = (Vote) c.decode(reencoded);
    logDecoded(d2, d2.getResponse().getMetadata());
    assertEquals(m, d2);
  }

  @Test
  public void testOutcomeCommitMixedMetadata() throws Exception {
    final Animal<?> a = new Dog().named("Rex").withFriend(new Cat().named("Tigger"));
    final Response ra = new Response("test-cohort-a", Intent.ACCEPT, a);
    final Response rb = new Response("test-cohort-b", Intent.ACCEPT, null);
    final Outcome m = new Outcome("O100", Verdict.COMMIT, null, new Response[] {ra, rb}).withSource("test");
    MessageCodec c;

    c = new KryoMessageCodec(false);
    final byte[] encoded = c.encode(m);
    logEncoded(encoded);
    
    final Outcome d1 = (Outcome) c.decode(encoded);
    logDecoded(d1, d1.getResponses()[0].getMetadata());
    assertEquals(2, d1.getResponses().length);
    assertNotNull(d1.getResponses()[0].getMetadata());
    assertEquals(PayloadBuffer.class, d1.getResponses()[0].getMetadata().getClass());
    assertNull(d1.getResponses()[1].getMetadata());
    
    final byte[] reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertArrayEquals(encoded, reencoded);
    
    c = new KryoMessageCodec(true);
    final Outcome d2 = (Outcome) c.decode(reencoded);
    logDecoded(d2, d2.getResponses()[0].getMetadata());
    assertEquals(m, d2);
  }

  @Test
  public void testOutcomeAbortMixedMetadata() throws Exception {
    final Animal<?> a = new Dog().named("Rex").withFriend(new Cat().named("Tigger"));
    final Response ra = new Response("test-cohort-a", Intent.REJECT, a);
    final Response rb = new Response("test-cohort-b", Intent.ACCEPT, null);
    final Outcome m = new Outcome("O100", Verdict.ABORT, AbortReason.REJECT, new Response[] {ra, rb});
    MessageCodec c;

    c = new KryoMessageCodec(false);
    final byte[] encoded = c.encode(m);
    logEncoded(encoded);
    
    final Outcome d1 = (Outcome) c.decode(encoded);
    logDecoded(d1, d1.getResponses()[0].getMetadata());
    assertEquals(2, d1.getResponses().length);
    assertNotNull(d1.getResponses()[0].getMetadata());
    assertEquals(PayloadBuffer.class, d1.getResponses()[0].getMetadata().getClass());
    assertNull(d1.getResponses()[1].getMetadata());
    
    final byte[] reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertArrayEquals(encoded, reencoded);
    
    c = new KryoMessageCodec(true);
    final Outcome d2 = (Outcome) c.decode(reencoded);
    logDecoded(d2, d2.getResponses()[0].getMetadata());
    assertEquals(m, d2);
  }
  
  @Test
  public void testUnknownSerialize() throws Exception {
    final UnknownMessage m = new UnknownMessage("U400");
    final MessageCodec c = new KryoMessageCodec(false);
    thrown.expect(MessageSerializationException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
    c.encode(m);
  }
  
  @Test
  public void testUnknownDeserialize() throws Exception {
    final MessageCodec c = new KryoMessageCodec(false);
    final Output buffer = new Output(16, -1);
    try {
      buffer.writeVarInt(Kryo.NOT_NULL, true);
      buffer.writeByte(MessageType.$UNKNOWN.ordinal());
      buffer.writeString(null);
      buffer.writeLong(0);
      buffer.writeString(null);
      final byte[] encoded = buffer.toBytes();
      thrown.expect(MessageDeserializationException.class);
      thrown.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
      c.decode(encoded);
    } finally {
      buffer.close();
    }
  }
  
  public static void main(String[] args) {
    TestBenchmark.setEnabled(KryoMessageCodecTest.class);
    JUnitCore.runClasses(KryoMessageCodecTest.class);
  }
}