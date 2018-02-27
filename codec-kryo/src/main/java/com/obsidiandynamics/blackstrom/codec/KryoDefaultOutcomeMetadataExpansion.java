package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.obsidiandynamics.blackstrom.codec.KryoMessageCodec.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class KryoDefaultOutcomeMetadataExpansion implements KryoExpansion {
  @Override
  public void accept(Kryo kryo) {
    kryo.addDefaultSerializer(DefaultOutcomeMetadata.class, KryoDefaultOutcomeMetadataSerializer.class);
  }
}
