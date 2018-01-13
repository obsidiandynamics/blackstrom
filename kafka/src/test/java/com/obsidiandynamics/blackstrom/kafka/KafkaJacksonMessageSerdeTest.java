package com.obsidiandynamics.blackstrom.kafka;

import static org.junit.Assert.*;

import java.util.*;

import org.apache.kafka.common.config.*;
import org.junit.*;

import com.obsidiandynamics.blackstrom.kafka.KafkaJacksonMessageDeserializer.*;
import com.obsidiandynamics.blackstrom.kafka.KafkaJacksonMessageSerializer.*;
import com.obsidiandynamics.blackstrom.model.*;

public class KafkaJacksonMessageSerdeTest {
  @Test
  public void testSerde() {
    final KafkaJacksonMessageSerializer serializer = new KafkaJacksonMessageSerializer();
    serializer.configure(Collections.emptyMap(), false);
    final Message m = new Nomination(100L, new String[0], null, 0);
    final byte[] encoded = serializer.serialize("test", m);
    serializer.close();
    
    final KafkaJacksonMessageDeserializer deserializer = new KafkaJacksonMessageDeserializer();
    deserializer.configure(Collections.singletonMap(KafkaJacksonMessageDeserializer.CONFIG_MAP_PAYLOAD, false), false);
    final Message decoded = deserializer.deserialize("test", encoded);
    deserializer.close();
    
    assertEquals(m, decoded);
  }

  @Test(expected=MessageSerializationException.class)
  public void testSerializationError() {
    final KafkaJacksonMessageSerializer serializer = new KafkaJacksonMessageSerializer();
    serializer.configure(Collections.emptyMap(), false);
    final Message m = new UnknownMessage(100L);
    serializer.serialize("test", m);
    serializer.close();
  }
  
  @Test(expected=MessageDeserializationException.class)
  public void testDeserializationError() {
    final KafkaJacksonMessageDeserializer deserializer = new KafkaJacksonMessageDeserializer();
    deserializer.configure(Collections.singletonMap(KafkaJacksonMessageDeserializer.CONFIG_MAP_PAYLOAD, false), false);
    deserializer.deserialize("test", "{invalid json}".getBytes());
    deserializer.close();
  }
  
  @Test(expected=ConfigException.class)
  public void testDeserializerConfigureError() {
    final KafkaJacksonMessageDeserializer deserializer = new KafkaJacksonMessageDeserializer();
    deserializer.configure(Collections.emptyMap(), false);
    deserializer.close();
  }
}
