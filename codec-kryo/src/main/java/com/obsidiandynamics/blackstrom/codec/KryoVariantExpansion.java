package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.obsidiandynamics.blackstrom.codec.KryoMessageCodec.*;

public final class KryoVariantExpansion implements KryoExpansion {
  @Override
  public void accept(Kryo kryo) {
    kryo.addDefaultSerializer(MonoVariant.class, new KryoMonoVariantSerializer());
    kryo.addDefaultSerializer(PolyVariant.class, new KryoPolyVariantSerializer());
    kryo.addDefaultSerializer(Nil.class, new KryoNilSerializer());
  }
}
