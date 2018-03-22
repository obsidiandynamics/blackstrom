package com.obsidiandynamics.blackstrom.util;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.indigo.util.*;

public final class NanoClockTest {
  @Test
  public void testConformance() throws Exception {
    Assertions.assertUtilityClassWellDefined(NanoClock.class);
  }

  @Test
  public void testTime() {
    final long now = System.currentTimeMillis();
    final long nanoNow = NanoClock.now();
    assertTrue("nanoNow=" + nanoNow + ", now=" + now, nanoNow >= now * 1_000_000L);
    TestSupport.sleep(1);
    final long after = System.currentTimeMillis();
    assertTrue("nanoNow=" + nanoNow + ", now=" + now + ", after=" + after, after * 1_000_000L >= nanoNow);
  }
}
