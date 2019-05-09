package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public final class KryoNilSerializer extends Serializer<Nil> {
  @Override
  public void write(Kryo kryo, Output output, Nil v) {}

  @Override
  public Nil read(Kryo kryo, Input input, Class<? extends Nil> type) {
    return Nil.getInstance();
  }
}
