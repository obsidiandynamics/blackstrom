package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public final class KryoPolyVariantSerializer extends Serializer<PolyVariant> {
  @Override
  public void write(Kryo kryo, Output output, PolyVariant v) {
    kryo.writeObject(output, v.getVariants());
  }

  @Override
  public PolyVariant read(Kryo kryo, Input input, Class<? extends PolyVariant> type) {
    final var variants = kryo.readObject(input, MonoVariant[].class);
    return new PolyVariant(variants);
  }
}
