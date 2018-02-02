package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.esotericsoftware.kryo.pool.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KryoMessageCodec implements MessageCodec {
  private static final int DEF_MESSAGE_BUFFER_SIZE = 128;
  
  private final KryoPool pool;
  
  private final KryoMessageSerializer messageSerializer;
  
  public KryoMessageCodec(boolean mapPayload) {
    messageSerializer = new KryoMessageSerializer(mapPayload);
    pool = new KryoPool.Builder(Kryo::new).softReferences().build();
  }
  
  @Override
  public byte[] encode(Message message) {
    final Kryo kryo = pool.borrow();
    try {
      final Output out = new Output(DEF_MESSAGE_BUFFER_SIZE, -1);
      kryo.writeObject(out, message, messageSerializer);
      return out.toBytes();
    } finally {
      pool.release(kryo);
    }
  }

  @Override
  public Message decode(byte[] bytes) {
    final Kryo kryo = pool.borrow();
    try {
      return kryo.readObject(new Input(bytes), Message.class, messageSerializer);
    } finally {
      pool.release(kryo);
    }
  }
}
