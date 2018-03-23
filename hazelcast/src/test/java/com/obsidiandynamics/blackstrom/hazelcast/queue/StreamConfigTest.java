package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;

import org.junit.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.store.*;

public final class StreamConfigTest {
  @Test
  public void testConfig() {
    final String name = "name";
    final int heapCapacity = 100;
    final long storeCapacity = 200;
    final long storeRetentionMillis = 1000;
    final int syncReplicas = 2;
    final int asyncReplicas = 3;
    final Class<? extends RingbufferStoreFactory<byte[]>> storeFactoryClass = HeapRingbufferStore.Factory.class;
    
    final StreamConfig config = new StreamConfig()
        .withName(name)
        .withHeapCapacity(heapCapacity)
        .withStoreCapacity(storeCapacity)
        .withStoreRetention(storeRetentionMillis)
        .withStoreFactoryClass(storeFactoryClass)
        .withSyncReplicas(syncReplicas)
        .withAsyncReplicas(asyncReplicas);
    assertEquals(name, config.getName());
    assertEquals(heapCapacity, config.getHeapCapacity());
    assertEquals(storeCapacity, config.getStoreCapacity());
    assertEquals(storeRetentionMillis, config.getStoreRetention());
    assertEquals(syncReplicas, config.getSyncReplicas());
    assertEquals(asyncReplicas, config.getAsyncReplicas());
    assertEquals(storeFactoryClass, config.getStoreFactoryClass());
    
    Assertions.assertToStringOverride(config);
  }
}
