package com.obsidiandynamics.blackstrom.hazelcast.elect;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

public final class LeaseViewTest {
  @Test
  public void testGetTenant() {
    final LeaseViewImpl v = new LeaseViewImpl();
    final UUID c = UUID.randomUUID();
    v.put("resource", new Lease(c, Long.MAX_VALUE));
    assertEquals(c, v.getTenant("resource"));
  }
  
  @Test
  public void testIsCurrentTenant() {
    final LeaseViewImpl v = new LeaseViewImpl();
    final UUID c = UUID.randomUUID();
    v.put("resource", new Lease(c, Long.MAX_VALUE));
    assertTrue(v.isCurrentTenant("resource", c));
  }
}
