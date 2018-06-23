package com.obsidiandynamics.blackstrom.factor;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;

public final class FactorTest {
  private static class ClassGroupFactor implements Factor, Groupable.ClassGroup, Initable.Nop, Disposable.Nop {}
  private static class NullGroupFactor implements Factor, Groupable.NullGroup, Initable.Nop, Disposable.Nop {}
  
  @Test
  public void testClassGroup() {
    assertEquals(ClassGroupFactor.class.getSimpleName(), new ClassGroupFactor().getGroupId());
  }

  @Test
  public void testNullGroup() {
    assertNull(new NullGroupFactor().getGroupId());
  }
}
