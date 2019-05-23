package com.obsidiandynamics.blackstrom.ledger;

import static java.util.Collections.*;
import static org.junit.Assert.*;

import org.junit.*;

public final class NopOffsetsResolverTest {
  @Test
  public void testSingletion() {
    assertSame(NopOffsetsResolver.getInstance(), NopOffsetsResolver.getInstance());
  }
  
  @Test
  public void testResolve() {
    assertEquals(emptyMap(), NopOffsetsResolver.getInstance().resolve("testGroup", emptySet()));
  }
}
