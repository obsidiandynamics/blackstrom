package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class KafkaMessageIdTest {
  @Test
  public void testGetters() {
    final KafkaMessageId messageId = new KafkaMessageId("test", 2, 400);
    assertEquals("test", messageId.getTopic());
    assertEquals(2, messageId.getPartition());
    assertEquals(400, messageId.getOffset());
  }
  
  @Test
  public void testEqualsHashCode() {
    final KafkaMessageId m1 = new KafkaMessageId("test", 2, 400);
    final KafkaMessageId m2 = new KafkaMessageId("test", 3, 400);
    final KafkaMessageId m3 = new KafkaMessageId("test", 2, 400);
    final KafkaMessageId m4 = m1;
    
    assertNotEquals(m1, m2);
    assertEquals(m1, m3);
    assertEquals(m1, m4);
    
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1.hashCode(), m3.hashCode());
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new KafkaMessageId("test", 2, 400));
  }
  
  @Test
  public void testFromRecord() {
    final ConsumerRecord<?, ?> record = new ConsumerRecord<>("topic", 0, 100, "key", "value");
    final KafkaMessageId messageId = KafkaMessageId.fromRecord(record);
    assertEquals(new KafkaMessageId("topic", 0, 100), messageId);
  }
  
  @Test
  public void testToOffset() {
    final KafkaMessageId messageId = new KafkaMessageId("topic", 0, 100);
    final Map<TopicPartition, OffsetAndMetadata> offset = messageId.toOffset();
    assertEquals(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(100)), offset);
  }
}
