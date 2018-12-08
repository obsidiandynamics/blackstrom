package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class KryoDefaultOutcomeMetadataSerializer extends Serializer<OutcomeMetadata> {
  @Override
  public void write(Kryo kryo, Output out, OutcomeMetadata metadata) {
    out.writeLong(metadata.getProposalTimestamp());
  }
  
  @Override
  public OutcomeMetadata read(Kryo kryo, Input in, Class<? extends OutcomeMetadata> type) {
    final long proposalTimestamp = in.readLong();
    return new OutcomeMetadata(proposalTimestamp);
  }
}
