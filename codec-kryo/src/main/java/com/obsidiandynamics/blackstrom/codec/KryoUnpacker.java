package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.esotericsoftware.kryo.util.*;

public final class KryoUnpacker implements Unpacker<KryoPackedForm> {
  private final Pool<Kryo> kryoPool;
  
  public KryoUnpacker(Pool<Kryo> kryoPool) {
    this.kryoPool = kryoPool;
  }

  @Override
  public Class<? extends KryoPackedForm> getPackedType() {
    return KryoPackedForm.class;
  }

  @Override
  public Object unpack(KryoPackedForm packed, Class<?> contentClass) {
    try (var input = new Input(packed.getBytes())) {
      final var kryo = kryoPool.obtain();
      try {
        return kryo.readObject(input, contentClass);
      } finally {
        kryoPool.free(kryo);
      }
    }
  }
}
