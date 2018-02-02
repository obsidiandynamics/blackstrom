package com.obsidiandynamics.blackstrom.factor;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class FailureModeTest {
  private static class TestFailureMode extends FailureMode {
    TestFailureMode(double probability) {
      super(probability);
    }

    @Override
    public FailureType getFailureType() {
      return null;
    }
  }
  
  @Test
  public void testProbabilityAlways() {
    final int runs = 100;
    final TestFailureMode f = new TestFailureMode(1);
    assertEquals(1, f.getProbability(), Double.MIN_VALUE);
    for (int i = 0; i < runs; i++) {
      assertTrue(f.isTime());
    }
  }
  
  @Test
  public void testProbabilityNever() {
    final int runs = 100;
    final TestFailureMode f = new TestFailureMode(0);
    for (int i = 0; i < runs; i++) {
      assertFalse(f.isTime());
    }
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new DelayedDuplicateDelivery(0, 0));
    Assertions.assertToStringOverride(new DelayedDelivery(0, 0));
    Assertions.assertToStringOverride(new DuplicateDelivery(0));
  }
}
