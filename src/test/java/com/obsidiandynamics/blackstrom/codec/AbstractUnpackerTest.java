package com.obsidiandynamics.blackstrom.codec;

import org.junit.*;

public abstract class AbstractUnpackerTest {
  private Unpacker<?> unpacker;
  
  @Before
  public final void before() {
    unpacker = getUnpacker();
  }
  
  protected abstract Unpacker<?> getUnpacker();
  
  protected abstract <T> T roundTrip(T obj);
}
