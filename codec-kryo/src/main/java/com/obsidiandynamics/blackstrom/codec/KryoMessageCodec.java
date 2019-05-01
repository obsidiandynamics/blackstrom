package com.obsidiandynamics.blackstrom.codec;

import java.util.function.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.esotericsoftware.kryo.util.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.yconf.*;

@Y
public class KryoMessageCodec implements MessageCodec {
  public static final int ENCODING_VERSION = 4;
  
  private static final int DEF_MESSAGE_BUFFER_SIZE = 128;
  
  private static final KryoExpansion[] DEF_EXPANSIONS = { new KryoDefaultOutcomeMetadataExpansion() };
  
  @FunctionalInterface
  public interface KryoExpansion extends Consumer<Kryo> {}
  
  private final Pool<Kryo> pool;
  
  private final KryoMessageSerializer messageSerializer;
  
  public KryoMessageCodec(@YInject(name="mapPayload") boolean mapPayload, 
                          @YInject(name="expansions") KryoExpansion... expansions) {
    messageSerializer = new KryoMessageSerializer(mapPayload);
    pool = new Pool<>(true, false) {
      @Override
      protected Kryo create () {
        final var kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        for (var expansion : DEF_EXPANSIONS) expansion.accept(kryo);
        for (var expansion : expansions) expansion.accept(kryo);
        return kryo;
      }
    };
  }
  
  private Kryo obtain() {
    return pool.obtain();
  }
  
  private void free(Kryo kryo) {
    pool.free(kryo);
  }
  
  @Override
  public byte[] encode(Message message) {
    final var kryo = obtain();
    try {
      final var out = new Output(DEF_MESSAGE_BUFFER_SIZE, -1);
      kryo.writeObject(out, message, messageSerializer);
      return out.toBytes();
    } finally {
      free(kryo);
    }
  }

  @Override
  public Message decode(byte[] bytes) {
    final var kryo = obtain();
    try {
      return kryo.readObject(new Input(bytes), Message.class, messageSerializer);
    } finally {
      free(kryo);
    }
  }
}
