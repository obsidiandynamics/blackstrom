package com.obsidiandynamics.blackstrom.util;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class SandboxTest {
  @Test
  public void testForInstance() {
    final Sandbox s = Sandbox.forInstance(this);
    final Proposal p = new Proposal("B0", new String[] {}, new Object(), 0);
    assertFalse(s.contains(p));
    p.withShardKey(s.key());
    assertTrue(s.contains(p));
  }

  @Test
  public void testForKey() {
    final Sandbox s = Sandbox.forKey("test");
    final Proposal p = new Proposal("B0", new String[] {}, new Object(), 0);
    assertFalse(s.contains(p));
    p.withShardKey(s.key());
    assertTrue(s.contains(p));
  }

  @Test
  public void testToString() {
    Assertions.assertToStringOverride(Sandbox.forKey("test"));
  }
}
