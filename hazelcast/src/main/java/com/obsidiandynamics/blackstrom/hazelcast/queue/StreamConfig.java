package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.config.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class StreamConfig {
  @YInject
  private String name;
  
  @YInject
  private int inMemCapacity = RingbufferConfig.DEFAULT_CAPACITY;
  
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

  int getInMemCapacity() {
    return inMemCapacity;
  }

  public StreamConfig withInMemCapacity(int inMemCapacity) {
    this.inMemCapacity = inMemCapacity;
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
    return StreamConfig.class.getSimpleName() + " [name=" + name + ", inMemCapacity=" + inMemCapacity + ", syncReplicas=" + syncReplicas
           + ", asyncReplicas=" + asyncReplicas + "]";
  }
}
