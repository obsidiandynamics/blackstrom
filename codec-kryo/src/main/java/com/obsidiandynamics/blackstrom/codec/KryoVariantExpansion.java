package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.obsidiandynamics.blackstrom.codec.KryoMessageCodec.*;

public final class KryoVariantExpansion implements KryoExpansion {
  @Override
  public void accept(Kryo kryo) {
    kryo.addDefaultSerializer(UniVariant.class, new KryoUniVariantSerializer());
    kryo.addDefaultSerializer(MultiVariant.class, new KryoMultiVariantSerializer());
    kryo.addDefaultSerializer(Nil.class, new KryoNilSerializer());
  }
}
