package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class KryoDefaultOutcomeMetadataSerializer extends Serializer<DefaultOutcomeMetadata> {
  @Override
  public void write(Kryo kryo, Output out, DefaultOutcomeMetadata metadata) {
    out.writeLong(metadata.getProposalTimestamp());
  }
  
  @Override
  public DefaultOutcomeMetadata read(Kryo kryo, Input in, Class<DefaultOutcomeMetadata> type) {
    final long proposalTimestamp = in.readLong();
    return new DefaultOutcomeMetadata(proposalTimestamp);
  }
}
