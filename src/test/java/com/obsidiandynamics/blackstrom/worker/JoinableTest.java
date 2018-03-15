package com.obsidiandynamics.blackstrom.worker;

import static org.junit.Assert.*;

import org.junit.*;

public final class JoinableTest {
  private static final class TestJoinable implements Joinable {
    private final boolean join;
    
    TestJoinable(boolean join) {
      this.join = join;
    }

    @Override
    public boolean join(long timeoutMillis) throws InterruptedException {
      return join;
    }
  }
  
  @Test
  public void testJoinAllPass() throws InterruptedException {
    final Joinable j = new TestJoinable(true);
    assertTrue(Joinable.joinAll(1_000, j));
  }
  
  @Test
  public void testJoinAllFail() throws InterruptedException {
    final Joinable j = new TestJoinable(false);
    assertFalse(Joinable.joinAll(1_000, j));
  }
  
  @Test
  public void testJoinAllOutOfTime() throws InterruptedException {
    final Joinable j = new TestJoinable(false);
    assertFalse(Joinable.joinAll(0, j));
  }
  
  @Test
  public void testNop() throws InterruptedException {
    assertTrue(Joinable.NOP.join(0));
  }
}
