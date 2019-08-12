package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.ledger.KafkaMessageDeserializer.*;
import com.obsidiandynamics.blackstrom.ledger.KafkaMessageSerializer.*;
import com.obsidiandynamics.blackstrom.model.*;

public class KafkaMessageSerdeTest {
  private String codecLocator;

  @Before
  public void before() {
    codecLocator = CodecRegistry.register(new JacksonMessageCodec(false));
  }

  @After
  public void after() {
    CodecRegistry.deregister(codecLocator);
  }

  @Test
  public void testSerde() {
    final Map<String, ?> configs = Map.of(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator);
    final KafkaMessageSerializer serializer = new KafkaMessageSerializer();
    serializer.configure(configs, false);
    final Message m = new Proposal("B100", new String[0], null, 0);
    final byte[] encoded = serializer.serialize("test", m);
    serializer.close();

    final KafkaMessageDeserializer deserializer = new KafkaMessageDeserializer();
    deserializer.configure(configs, false);
    final Message decoded = deserializer.deserialize("test", encoded);
    deserializer.close();

    assertEquals(m, decoded);
  }

  @Test(expected=MessageSerializationException.class)
  public void testSerializationError() {
    final Map<String, ?> configs = Map.of(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator);
    final KafkaMessageSerializer serializer = new KafkaMessageSerializer();
    serializer.configure(configs, false);
    final Message m = new UnknownMessage("B100");
    serializer.serialize("test", m);
    serializer.close();
  }

  @Test(expected=MessageDeserializationException.class)
  public void testDeserializationError() {
    final Map<String, ?> configs = Map.of(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator);
    final KafkaMessageDeserializer deserializer = new KafkaMessageDeserializer();
    deserializer.configure(configs, false);
    deserializer.deserialize("test", "{invalid json}".getBytes());
    deserializer.close();
  }
}
