package com.obsidiandynamics.blackstrom.retention;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.flow.*;

public final class NopRetentionTest {
  @Test
  public void test() {
    final NopRetention r = NopRetention.getInstance();
    final Confirmation c = r.begin(null, null);
    assertNotNull(c);
  }
}
