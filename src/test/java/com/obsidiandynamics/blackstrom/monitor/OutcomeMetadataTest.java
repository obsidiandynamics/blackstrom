package com.obsidiandynamics.blackstrom.monitor;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class OutcomeMetadataTest {
  @Test
  public void testFields() {
    final OutcomeMetadata meta = new OutcomeMetadata(100);
    assertEquals(100L, meta.getProposalTimestamp());
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(OutcomeMetadata.class).verify();
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new OutcomeMetadata(100));
  }
}
