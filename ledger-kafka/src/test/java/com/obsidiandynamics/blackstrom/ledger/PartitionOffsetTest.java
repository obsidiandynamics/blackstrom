package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.assertj.core.api.*;
import org.junit.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class PartitionOffsetTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(PartitionOffset.class).excludeToStringFields().verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(PartitionOffset.class).verify();
  }
  
  @Test
  public void testToString() {
    assertEquals("10#99999", new PartitionOffset(10, 99999).toString());
  }
  
  @Test
  public void testSummarise() {
    final var map = MapBuilder
        .init(new TopicPartition("testTopic", 2), new OffsetAndMetadata(200))
        .with(new TopicPartition("testTopic", 3), new OffsetAndMetadata(300))
        .with(new TopicPartition("testTopic", 1), new OffsetAndMetadata(100))
        .build();
    
    final var summary = PartitionOffset.summarise(map);
    Assertions.assertThat(summary).containsExactly(new PartitionOffset(1, 100), new PartitionOffset(2, 200), new PartitionOffset(3, 300));
  }
}
