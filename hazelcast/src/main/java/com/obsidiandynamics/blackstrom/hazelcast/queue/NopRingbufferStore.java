package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.io.*;
import java.util.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.yconf.*;

public final class NopRingbufferStore implements RingbufferStore<byte[]> {
  @Y(Factory.Mapper.class)
  public static final class Factory implements RingbufferStoreFactory<byte[]>, Serializable {
    public static final class Mapper implements TypeMapper {
      @Override public Object map(YObject y, Class<?> type) {
        return instance;
      }
    }
    
    private static final long serialVersionUID = 1L;
    
    private static final Factory instance = new Factory();
    
    public static Factory getInstance() { return instance; };
    
    @Override
    public NopRingbufferStore newRingbufferStore(String name, Properties properties) {
      return NopRingbufferStore.instance;
    }
  }
  
  private static final NopRingbufferStore instance = new NopRingbufferStore();
  
  private NopRingbufferStore() {}

  @Override
  public void store(long sequence, byte[] data) {}

  @Override
  public void storeAll(long firstItemSequence, byte[][] items) {}

  @Override
  public byte[] load(long sequence) {
    return null;
  }

  @Override
  public long getLargestSequence() {
    return -1;
  }
}
