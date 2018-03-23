package com.obsidiandynamics.blackstrom.hazelcast.queue.store;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.hazelcast.queue.store.NopRingbufferStore.*;
import com.obsidiandynamics.yconf.*;

public final class NopRingbufferStoreTest {
  @Test
  public void testConfig() throws Exception {
    assertNotNull(new MappingContext().withParser(__reader -> new Object()).fromString("").map(Factory.class));
  }
  
  @Test
  public void testMethods() {
    final NopRingbufferStore store = NopRingbufferStore.Factory.getInstance().newRingbufferStore("store", new Properties());
    store.store(0, null);
    store.storeAll(0, null);
    assertNull(store.load(0));
    assertEquals(-1, store.getLargestSequence());
  }
}
