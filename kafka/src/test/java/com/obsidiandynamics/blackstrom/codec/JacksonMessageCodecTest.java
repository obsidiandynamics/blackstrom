package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public class JacksonMessageCodecTest implements TestSupport {
  private static void logEncoded(String encoded) {
    if (LOG) LOG_STREAM.format("encoded %s\n", encoded);
  }
  
  private static void logDecoded(Message m, Object p) {
    if (LOG) LOG_STREAM.format("decoded %s (type=%s)\n", m, (p != null ? p.getClass().getSimpleName() : "n/a"));
  }
  
  @Test
  public void testNominationNullProposal() throws Exception {
    final Message m = new Nomination("N100", new String[] {"a",  "b"}, null, 1000).withSource("test");
    MessageCodec c;
    
    c = new JacksonMessageCodec(false);
    final String encoded = c.encode(m);
    logEncoded(encoded);

    final Nomination d1 = (Nomination) c.decode(encoded);
    logDecoded(d1, d1.getProposal());
    assertEquals(m, d1);

    c = new JacksonMessageCodec(true);
    final Nomination d2 = (Nomination) c.decode(encoded);
    logDecoded(d2, d2.getProposal());
    assertEquals(m, d2);
  }

  @Test
  public void testNominationNonNullProposal() throws Exception {
    final Animal<?> a = new Dog().named("Rover").withFriend(new Cat().named("Misty"));
    final Nomination m = new Nomination("N100", new String[] {"a",  "b"}, a, 1000);
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encode(m);
    logEncoded(encoded);
    
    final Nomination d1 = (Nomination) c.decode(encoded);
    logDecoded(d1, d1.getProposal());
    assertNotNull(d1.getProposal());
    assertEquals(LinkedHashMap.class, d1.getProposal().getClass());
    
    c = new JacksonMessageCodec(true);
    final Nomination d2 = (Nomination) c.decode(encoded);
    logDecoded(d2, d2.getProposal());
    assertEquals(m, d2);
  }
}
