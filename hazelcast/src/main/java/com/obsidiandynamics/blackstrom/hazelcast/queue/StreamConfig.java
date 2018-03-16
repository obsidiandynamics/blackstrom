package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.concurrent.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class StreamConfig {
  @YInject
  private String name;
  
  @YInject
  private int heapCapacity = RingbufferConfig.DEFAULT_CAPACITY;
  
  @YInject
  private long residualCapacity = Long.MAX_VALUE;
  
  @YInject
  private long residualRetentionMillis = TimeUnit.DAYS.toMillis(7);
  
  @YInject
  private RingbufferStoreFactory<byte[]> residualStoreFactory = NopRingbufferStore.Factory.getInstance();
  
  @YInject
  private int syncReplicas = RingbufferConfig.DEFAULT_SYNC_BACKUP_COUNT;
  
  @YInject
  private int asyncReplicas = RingbufferConfig.DEFAULT_ASYNC_BACKUP_COUNT;

  String getName() {
    return name;
  }

  public StreamConfig withName(String name) {
    this.name = name;
    return this;
  }

  int getHeapCapacity() {
    return heapCapacity;
  }

  public StreamConfig withHeapCapacity(int heapCapacity) {
    this.heapCapacity = heapCapacity;
    return this;
  }

  long getResidualCapacity() {
    return residualCapacity;
  }

  public StreamConfig withResidualCapacity(long residualCapacity) {
    this.residualCapacity = residualCapacity;
    return this;
  }

  long getResidualRetention() {
    return residualRetentionMillis;
  }

  public StreamConfig withResidualRetention(long residualRetentionMillis) {
    this.residualRetentionMillis = residualRetentionMillis;
    return this;
  }

  RingbufferStoreFactory<byte[]> getResidualStoreFactory() {
    return residualStoreFactory;
  }

  public StreamConfig withResidualStoreFactory(RingbufferStoreFactory<byte[]> residualStoreFactory) {
    this.residualStoreFactory = residualStoreFactory;
    return this;
  }

  int getSyncReplicas() {
    return syncReplicas;
  }

  public StreamConfig withSyncReplicas(int syncReplicas) {
    this.syncReplicas = syncReplicas;
    return this;
  }

  int getAsyncReplicas() {
    return asyncReplicas;
  }

  public StreamConfig withAsyncReplicas(int asyncReplicas) {
    this.asyncReplicas = asyncReplicas;
    return this;
  }

  @Override
  public String toString() {
    return StreamConfig.class.getSimpleName() + " [name=" + name + ", heapCapacity=" + heapCapacity + ", residualCapacity=" + residualCapacity
           + ", residualRetentionMillis=" + residualRetentionMillis + ", residualStoreFactory=" + residualStoreFactory
           + ", syncReplicas=" + syncReplicas + ", asyncReplicas=" + asyncReplicas + "]";
  }
}
