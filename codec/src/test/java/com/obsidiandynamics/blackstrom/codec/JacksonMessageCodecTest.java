package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import java.util.*;

import org.hamcrest.core.*;
import org.junit.*;
import org.junit.rules.*;

import com.fasterxml.jackson.databind.*;
import com.obsidiandynamics.blackstrom.codec.JacksonMessageDeserializer.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public class JacksonMessageCodecTest implements TestSupport {
  private static void logEncoded(String encoded) {
    if (LOG) LOG_STREAM.format("encoded %s\n", encoded);
  }
  
  private static void logReencoded(String reencoded) {
    if (LOG) LOG_STREAM.format("re-encoded %s\n", reencoded);
  }
  
  private static void logDecoded(Message m, Object p) {
    if (LOG) LOG_STREAM.format("decoded %s (type=%s)\n", m, (p != null ? p.getClass().getSimpleName() : "n/a"));
  }
  
  @Rule 
  public ExpectedException thrown = ExpectedException.none();
  
  @Test
  public void testNominationNullProposal() throws Exception {
    final Message m = new Nomination("N100", new String[] {"a", "b"}, null, 1000).withSource("test");
    MessageCodec c;
    
    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);

    final Nomination d1 = (Nomination) c.decodeText(encoded);
    logDecoded(d1, d1.getProposal());
    assertEquals(m, d1);
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);

    c = new JacksonMessageCodec(true);
    final Nomination d2 = (Nomination) c.decodeText(reencoded);
    logDecoded(d2, d2.getProposal());
    assertEquals(m, d2);
  }
  
  @Test
  public void testCodecBenchmark() throws Exception {
    final int runs = 100;
    final Message m = new Nomination("N100", new String[] {"a", "b"}, null, 1000).withSource("test");
    final MessageCodec c = new JacksonMessageCodec(false);
    
    final long took = TestSupport.tookThrowing(() -> {
      for (int i = 0; i < runs; i++) {
        final String encoded = c.encodeText(m);
        c.decodeText(encoded);
      }
    });
    
    System.out.format("Codec: %,d took %,d ms, %,.0f msgs/sec\n", 
                      runs, took, (float) runs / took * 1000);
  }
  
  @Test
  public void testNominationLongBallotId() throws Exception {
    final Message m = new Nomination(101L, new String[] {"a", "b"}, null, 1000).withSource("test");
    MessageCodec c;
    
    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);

    final Nomination d1 = (Nomination) c.decodeText(encoded);
    logDecoded(d1, d1.getProposal());
    assertEquals(m, d1);
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);

    c = new JacksonMessageCodec(true);
    final Nomination d2 = (Nomination) c.decodeText(reencoded);
    logDecoded(d2, d2.getProposal());
    assertEquals(m, d2);
  }

  @Test
  public void testNominationNonNullProposal() throws Exception {
    final Animal<?> a = new Dog().named("Rover").withFriend(new Cat().named("Misty"));
    final Nomination m = new Nomination("N100", new String[] {"a", "b"}, a, 1000);
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);
    
    final Nomination d1 = (Nomination) c.decodeText(encoded);
    logDecoded(d1, d1.getProposal());
    assertNotNull(d1.getProposal());
    assertEquals(LinkedHashMap.class, d1.getProposal().getClass());
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Nomination d2 = (Nomination) c.decodeText(reencoded);
    logDecoded(d2, d2.getProposal());
    assertEquals(m, d2);
  }

  @Test
  public void testVoteNonNullMetadata() throws Exception {
    final Animal<?> a = new Dog().named("Rex").withFriend(new Cat().named("Tigger"));
    final Vote m = new Vote("V100", new Response("test-cohort", Pledge.ACCEPT, a));
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);
    
    final Vote d1 = (Vote) c.decodeText(encoded);
    logDecoded(d1, d1.getResponse().getMetadata());
    assertNotNull(d1.getResponse().getMetadata());
    assertEquals(LinkedHashMap.class, d1.getResponse().getMetadata().getClass());
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Vote d2 = (Vote) c.decodeText(reencoded);
    logDecoded(d2, d2.getResponse().getMetadata());
    assertEquals(m, d2);
  }

  @Test
  public void testOutcomeCommitMixedMetadata() throws Exception {
    final Animal<?> a = new Dog().named("Rex").withFriend(new Cat().named("Tigger"));
    final Response ra = new Response("test-cohort-a", Pledge.ACCEPT, a);
    final Response rb = new Response("test-cohort-b", Pledge.ACCEPT, null);
    final Outcome m = new Outcome("O100", Verdict.COMMIT, null, new Response[] {ra, rb});
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);
    
    final Outcome d1 = (Outcome) c.decodeText(encoded);
    logDecoded(d1, d1.getResponses()[0].getMetadata());
    assertEquals(2, d1.getResponses().length);
    assertNotNull(d1.getResponses()[0].getMetadata());
    assertEquals(LinkedHashMap.class, d1.getResponses()[0].getMetadata().getClass());
    assertNull(d1.getResponses()[1].getMetadata());
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Outcome d2 = (Outcome) c.decodeText(reencoded);
    logDecoded(d2, d2.getResponses()[0].getMetadata());
    assertEquals(m, d2);
  }

  @Test
  public void testOutcomeAbortMixedMetadata() throws Exception {
    final Animal<?> a = new Dog().named("Rex").withFriend(new Cat().named("Tigger"));
    final Response ra = new Response("test-cohort-a", Pledge.REJECT, a);
    final Response rb = new Response("test-cohort-b", Pledge.ACCEPT, null);
    final Outcome m = new Outcome("O100", Verdict.ABORT, AbortReason.REJECT, new Response[] {ra, rb});
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encodeText(m);
    logEncoded(encoded);
    
    final Outcome d1 = (Outcome) c.decodeText(encoded);
    logDecoded(d1, d1.getResponses()[0].getMetadata());
    assertEquals(2, d1.getResponses().length);
    assertNotNull(d1.getResponses()[0].getMetadata());
    assertEquals(LinkedHashMap.class, d1.getResponses()[0].getMetadata().getClass());
    assertNull(d1.getResponses()[1].getMetadata());
    
    final String reencoded = c.encodeText(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Outcome d2 = (Outcome) c.decodeText(reencoded);
    logDecoded(d2, d2.getResponses()[0].getMetadata());
    assertEquals(m, d2);
  }
  
  @Test
  public void testUnknownSerialize() throws Exception {
    final UnknownMessage m = new UnknownMessage("U400");
    final MessageCodec c = new JacksonMessageCodec(false);
    thrown.expect(JsonMappingException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
    c.encode(m);
  }
  
  @Test
  public void testUnknownDeserialize() throws Exception {
    final MessageCodec c = new JacksonMessageCodec(false);
    final String encoded = "{\"messageType\":\"$UNKNOWN\",\"ballotId\":\"$U400\",\"timestamp\":1000}";
    thrown.expect(MessageDeserializationException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
    c.decodeText(encoded);
  }
}
