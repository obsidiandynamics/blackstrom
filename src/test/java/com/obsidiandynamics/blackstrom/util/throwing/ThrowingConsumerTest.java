package com.obsidiandynamics.blackstrom.util.throwing;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.*;

import org.junit.*;

public final class ThrowingConsumerTest {
  @Test
  public void testAccept() throws Exception {
    final AtomicReference<String> consumed0 = new AtomicReference<>();
    final AtomicReference<String> consumed1 = new AtomicReference<>();
    final ThrowingConsumer<String> c = consumed0::set;
    final ThrowingConsumer<String> cc = c.andThen(consumed1::set);
    cc.accept("test");
    
    assertEquals("test", consumed0.get());
    assertEquals("test", consumed1.get());
  }
}
