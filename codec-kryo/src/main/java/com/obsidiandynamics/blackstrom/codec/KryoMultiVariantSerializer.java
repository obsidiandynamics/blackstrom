package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public final class KryoMultiVariantSerializer extends Serializer<MultiVariant> {
  @Override
  public void write(Kryo kryo, Output output, MultiVariant v) {
    kryo.writeObject(output, v.getVariants());
  }

  @Override
  public MultiVariant read(Kryo kryo, Input input, Class<? extends MultiVariant> type) {
    final var variants = kryo.readObject(input, UniVariant[].class);
    return new MultiVariant(variants);
  }
}
