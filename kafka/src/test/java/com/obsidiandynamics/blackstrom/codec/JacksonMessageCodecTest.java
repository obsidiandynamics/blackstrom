package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import org.apache.commons.lang3.builder.*;
import org.hamcrest.core.*;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.mockito.*;
import org.powermock.api.mockito.*;
import org.powermock.core.classloader.annotations.*;
import org.powermock.modules.junit4.*;
import org.powermock.reflect.*;

import com.fasterxml.jackson.databind.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(MessageType.class)
public class JacksonMessageCodecTest implements TestSupport {
  private static MessageType UNKNOWN;
  
  @BeforeClass
  public static void beforeClass() {
    try {
      UNKNOWN = PowerMockito.mock(MessageType.class);
      Whitebox.setInternalState(UNKNOWN, "name", "UNKNOWN");
      Whitebox.setInternalState(UNKNOWN, "ordinal", MessageType.values().length);
      final MessageType[] messageTypes = new MessageType[MessageType.values().length + 1];
      System.arraycopy(MessageType.values(), 0, messageTypes, 0, MessageType.values().length);
      messageTypes[messageTypes.length - 1] = UNKNOWN;

      final Map<String, MessageType> messageTypesMap = Arrays.stream(messageTypes)
          .collect(Collectors.toMap(mt -> mt.name(), Function.identity()));
          
      PowerMockito.mockStatic(MessageType.class);
      PowerMockito.when(MessageType.values()).thenReturn(messageTypes);
      PowerMockito.when(MessageType.valueOf(Mockito.isNotNull())).thenAnswer(invocation -> {
        final String name = invocation.getArgument(0);
        return messageTypesMap.get(name);
      });
      
      PowerMockito.mockStatic(Enum.class);
      PowerMockito.when(Enum.valueOf(MessageType.class, "NOMINATION")).thenReturn(MessageType.NOMINATION);
      PowerMockito.when(Enum.valueOf(MessageType.class, "VOTE")).thenReturn(MessageType.VOTE);
      PowerMockito.when(Enum.valueOf(MessageType.class, "OUTCOME")).thenReturn(MessageType.OUTCOME);
      PowerMockito.when(Enum.valueOf(MessageType.class, "UNKNOWN")).thenReturn(UNKNOWN);
      
      PowerMockito.when(UNKNOWN.toString()).thenReturn("UNKNOWN");
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
  
  public static final class UnknownMessage extends Message {
    public UnknownMessage(Object ballotId) {
      this(ballotId, 0);
    }
    
    public UnknownMessage(Object ballotId, long timestamp) {
      super(ballotId, timestamp);
    }

    @Override
    public MessageType getMessageType() {
      return UNKNOWN;
    }
    
    @Override
    public int hashCode() {
      return new HashCodeBuilder()
          .appendSuper(super.hashCode())
          .toHashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj instanceof UnknownMessage) {
        return new EqualsBuilder()
            .appendSuper(super.equals(obj))
            .isEquals();
      } else {
        return false;
      }
    }
    
    @Override
    public String toString() {
      return UnknownMessage.class.getSimpleName() + " [" + baseToString() + "]";
    }
  }
  
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
    final Message m = new Nomination("N100", new String[] {"a",  "b"}, null, 1000).withSource("test");
    MessageCodec c;
    
    c = new JacksonMessageCodec(false);
    final String encoded = c.encode(m);
    logEncoded(encoded);

    final Nomination d1 = (Nomination) c.decode(encoded);
    logDecoded(d1, d1.getProposal());
    assertEquals(m, d1);
    
    final String reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);

    c = new JacksonMessageCodec(true);
    final Nomination d2 = (Nomination) c.decode(reencoded);
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
    
    final String reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Nomination d2 = (Nomination) c.decode(reencoded);
    logDecoded(d2, d2.getProposal());
    assertEquals(m, d2);
  }

  @Test
  public void testVoteNonNullMetadata() throws Exception {
    final Animal<?> a = new Dog().named("Rex").withFriend(new Cat().named("Tigger"));
    final Vote m = new Vote("V100", new Response("test-cohort", Pledge.ACCEPT, a));
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encode(m);
    logEncoded(encoded);
    
    final Vote d1 = (Vote) c.decode(encoded);
    logDecoded(d1, d1.getResponse().getMetadata());
    assertNotNull(d1.getResponse().getMetadata());
    assertEquals(LinkedHashMap.class, d1.getResponse().getMetadata().getClass());
    
    final String reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Vote d2 = (Vote) c.decode(reencoded);
    logDecoded(d2, d2.getResponse().getMetadata());
    assertEquals(m, d2);
  }

  @Test
  public void testOutcomeMixedMetadata() throws Exception {
    final Animal<?> a = new Dog().named("Rex").withFriend(new Cat().named("Tigger"));
    final Response ra = new Response("test-cohort-a", Pledge.REJECT, a);
    final Response rb = new Response("test-cohort-b", Pledge.ACCEPT, null);
    final Outcome m = new Outcome("O100", Verdict.ABORT, new Response[] {ra, rb});
    MessageCodec c;

    c = new JacksonMessageCodec(false);
    final String encoded = c.encode(m);
    logEncoded(encoded);
    
    final Outcome d1 = (Outcome) c.decode(encoded);
    logDecoded(d1, d1.getResponses()[0].getMetadata());
    assertEquals(2, d1.getResponses().length);
    assertNotNull(d1.getResponses()[0].getMetadata());
    assertEquals(LinkedHashMap.class, d1.getResponses()[0].getMetadata().getClass());
    assertNull(d1.getResponses()[1].getMetadata());
    
    final String reencoded = c.encode(d1);
    logReencoded(reencoded);
    assertEquals(encoded, reencoded);
    
    c = new JacksonMessageCodec(true);
    final Outcome d2 = (Outcome) c.decode(reencoded);
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
  
//  @Test
//  public void testUnknownDeserialize() throws Exception {
//    final MessageCodec c = new JacksonMessageCodec(false);
//    final String encoded = "{\"messageType\":\"UNKNOWN\"}";
//    thrown.expect(JsonMappingException.class);
//    thrown.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
//    c.decode(encoded);
//  }
}
