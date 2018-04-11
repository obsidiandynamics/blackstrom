package com.obsidiandynamics.blackstrom.util;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class HashTest {
  @Test
  public void testConformance() {
    Assertions.assertUtilityClassWellDefined(Hash.class);
  }
  
  @Test
  public void testFold() {
    assertEquals(0, Hash.fold(0));
    assertEquals(1, Hash.fold(1));
    assertEquals(Integer.MAX_VALUE, Hash.fold(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, Hash.fold(-1));
    assertEquals(0, Hash.fold(Integer.MIN_VALUE));
  }
  
  @Test
  public void testForShardKey() {
    final String key = "a";
    final int hash = key.hashCode();
    final Message m = new UnknownMessage("B0").withShardKey(key);
    assertEquals(hash, Hash.getShard(m, hash + 1));
    assertEquals(0, Hash.getShard(m, hash));
  }
  
  @Test
  public void testNullShardKey() {
    assertEquals(0, Hash.getShard(new UnknownMessage("B0"), 1));
  }
  
  @Test
  public void testAssignedShard() {
    final Message m = new UnknownMessage("B0").withShard(5);
    assertEquals(5, Hash.getShard(m, 6));
  }
  
  @Test(expected=IndexOutOfBoundsException.class)
  public void testAssignedShardOutOfBounds() {
    final Message m = new UnknownMessage("B0").withShard(5);
    assertEquals(5, Hash.getShard(m, 5));
  }
}
