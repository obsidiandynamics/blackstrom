package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

public final class NopOffsetsResolverTest {
  @Test
  public void testSingletion() {
    assertSame(NopOffsetsResolver.getInstance(), NopOffsetsResolver.getInstance());
  }
  
  @Test
  public void testResolve() {
    assertEquals(Collections.emptyMap(), NopOffsetsResolver.getInstance().resolve("testGroup"));
  }
}
