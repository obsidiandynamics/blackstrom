package com.obsidiandynamics.blackstrom.handler;

import static org.junit.Assert.*;

import org.junit.*;

public final class FactorTest {
  private static class ClassGroupFactor implements Factor, Groupable.ClassGroup {}
  private static class NullGroupFactor implements Factor, Groupable.NullGroup {}
  
  @Test
  public void testClassGroup() {
    assertEquals(ClassGroupFactor.class.getSimpleName(), new ClassGroupFactor().getGroupId());
  }

  @Test
  public void testNullGroup() {
    assertNull(new NullGroupFactor().getGroupId());
  }
}
