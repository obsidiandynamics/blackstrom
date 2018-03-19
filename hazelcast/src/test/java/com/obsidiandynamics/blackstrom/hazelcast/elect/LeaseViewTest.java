package com.obsidiandynamics.blackstrom.hazelcast.elect;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class LeaseViewTest {
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new LeaseViewImpl(10));
  }
  
  @Test
  public void testViewVersion() {
    assertEquals(10, new LeaseViewImpl(10).getVersion());
  }
  
  @Test
  public void testGetTenant() {
    final LeaseViewImpl v = new LeaseViewImpl(0);
    final UUID c = UUID.randomUUID();
    v.put("resource", new Lease(c, Long.MAX_VALUE));
    assertEquals(c, v.getLease("resource").getTenant());
  }
  
  @Test
  public void testIsCurrentTenant() {
    final LeaseViewImpl v = new LeaseViewImpl(0);
    final UUID c = UUID.randomUUID();
    v.put("resource", new Lease(c, Long.MAX_VALUE));
    assertTrue(v.isCurrentTenant("resource", c));
  }
}
