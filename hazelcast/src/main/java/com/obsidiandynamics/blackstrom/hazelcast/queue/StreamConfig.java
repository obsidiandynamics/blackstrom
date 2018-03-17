package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;
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
  private long storeCapacity = Long.MAX_VALUE;
  
  @YInject
  private long storeRetentionMillis = TimeUnit.DAYS.toMillis(7);
  
  @YInject
  private Class<? extends RingbufferStoreFactory<byte[]>> storeFactoryClass = NopRingbufferStore.Factory.class;
  
  @YInject
  private int syncReplicas = RingbufferConfig.DEFAULT_SYNC_BACKUP_COUNT;
  
  @YInject
  private int asyncReplicas = RingbufferConfig.DEFAULT_ASYNC_BACKUP_COUNT;

  @YInject
  private Properties storeFactoryProps = new Properties();

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

  long getStoreCapacity() {
    return storeCapacity;
  }

  public StreamConfig withStoreCapacity(long storeCapacity) {
    this.storeCapacity = storeCapacity;
    return this;
  }

  long getStoreRetention() {
    return storeRetentionMillis;
  }

  public StreamConfig withStoreRetention(long storeRetentionMillis) {
    this.storeRetentionMillis = storeRetentionMillis;
    return this;
  }

  Class<? extends RingbufferStoreFactory<byte[]>> getStoreFactoryClass() {
    return storeFactoryClass;
  }

  public StreamConfig withStoreFactoryClass(Class<? extends RingbufferStoreFactory<byte[]>> storeFactoryClass) {
    this.storeFactoryClass = storeFactoryClass;
    return this;
  }

  Properties getStoreFactoryProps() {
    return storeFactoryProps;
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
    return StreamConfig.class.getSimpleName() + " [name=" + name + ", heapCapacity=" + heapCapacity + ", residualCapacity=" + storeCapacity
           + ", storeRetentionMillis=" + storeRetentionMillis + ", storeFactoryClass=" + storeFactoryClass
           + ", syncReplicas=" + syncReplicas + ", asyncReplicas=" + asyncReplicas + "]";
  }
}
