package com.obsidiandynamics.blackstrom.hazelcast;

import java.util.concurrent.atomic.*;
import java.util.function.*;

import com.hazelcast.core.*;

public final class InstancePool {
  private final Supplier<HazelcastInstance> instanceSupplier;
  
  private final AtomicReferenceArray<HazelcastInstance> instances;
  
  private final AtomicInteger position = new AtomicInteger();
  
  public InstancePool(int size, Supplier<HazelcastInstance> instanceSupplier) {
    this.instanceSupplier = instanceSupplier;
    instances = new AtomicReferenceArray<>(size);
  }
  
  public HazelcastInstance get() {
    final int index = position.getAndIncrement() % instances.length();
    return instances.updateAndGet(index, instance -> instance != null ? instance : instanceSupplier.get());
  }
}
