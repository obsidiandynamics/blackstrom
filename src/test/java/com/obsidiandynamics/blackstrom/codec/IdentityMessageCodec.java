package com.obsidiandynamics.blackstrom.codec;

import java.nio.*;
import java.util.*;

import com.obsidiandynamics.blackstrom.model.*;

/**
 *  A test message codec that caches a shallow clone of the {@link Message} object and returns a 
 *  serialised form of its unique ({@code int}) identity (key in a hashmap). The same 
 *  {@link IdentityMessageCodec} instance <em>must</em> therefore be used for deserialisation.
 */
public final class IdentityMessageCodec implements MessageCodec {
  private final Map<Integer, Message> messages = new HashMap<>();
  
  private int nextSerialId;

  @Override
  public byte[] encode(Message message) throws Exception {
    final int serialId = nextSerialId++;
    messages.put(serialId, cloneMessage(message));
    final ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putInt(serialId);
    return buf.array();
  }
  
  private static Message cloneMessage(Message original) {
    final Message clone;
    switch (original.getMessageType()) {
      case PROPOSAL:
        clone = cloneProposal((Proposal) original);
        break;
        
      case VOTE:
        clone = cloneVote((Vote) original);
        break;
        
      case OUTCOME:
        clone = cloneOutcome((Outcome) original);
        break;
        
      case $UNKNOWN:
      default:
        throw new UnsupportedOperationException("Cannot clone message of type " + original.getMessageType());
    }
    
    cloneBaseFields(original, clone);
    return clone;
  }

  private static Message cloneProposal(Proposal original) {
    return new Proposal(original.getBallotId(), original.getTimestamp(), original.getCohorts(), original.getObjective(), 
                        original.getTtl());
  }

  private static Message cloneVote(Vote original) {
    return new Vote(original.getBallotId(), original.getTimestamp(), original.getResponse());
  }
  
  private static Message cloneOutcome(Outcome original) {
    return new Outcome(original.getBallotId(), original.getTimestamp(), original.getResolution(), original.getAbortReason(), 
                       original.getResponses(), original.getMetadata());
  }

  private static void cloneBaseFields(Message original, Message clone) {
    clone.setShardKey(original.getShardKey());
    clone.setShard(original.getShard());
    clone.setSource(original.getSource());
  }

  @Override
  public Message decode(byte[] bytes) throws Exception {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int serialId = buf.getInt();
    return messages.get(serialId);
  }
}
