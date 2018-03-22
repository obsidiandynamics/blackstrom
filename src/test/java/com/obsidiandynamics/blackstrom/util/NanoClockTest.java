package com.obsidiandynamics.blackstrom.util;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class NanoClockTest {
  @Test
  public void testConformance() throws Exception {
    Assertions.assertUtilityClassWellDefined(NanoClock.class);
  }

  @Test
  public void testTime() {
    assertTrue(NanoClock.now() > 0);
  }
}
