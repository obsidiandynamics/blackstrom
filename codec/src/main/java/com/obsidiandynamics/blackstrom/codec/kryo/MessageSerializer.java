package com.obsidiandynamics.blackstrom.codec.kryo;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.model.*;

final class MessageSerializer extends Serializer<Message> {
  static final class MessageSerializationException extends KryoException {
    private static final long serialVersionUID = 1L;
    
    MessageSerializationException(Throwable cause) {
      super("Error serializing message", cause);
    }
  }
  
  private final boolean mapPayload;
  
  MessageSerializer(boolean mapPayload) {
    this.mapPayload = mapPayload;
  }
  
  @Override
  public void write(Kryo kryo, Output out, Message message) {
    out.writeByte(message.getMessageType().ordinal());
    out.writeString(message.getBallotId());
    out.writeLong(message.getTimestamp());
    out.writeString(message.getSource());
    
    switch (message.getMessageType()) {
      case PROPOSAL:
        serializeProposal(kryo, out, (Proposal) message);
        break;
        
//      case VOTE:
//        serializeVote((Vote) message, gen);
//        break;
//        
//      case OUTCOME:
//        serializeOutcome((Outcome) message, gen);
//        break;
        
      case $UNKNOWN:
      default:
        final Throwable cause = new UnsupportedOperationException("Cannot serialize message of type " + message.getMessageType());
        throw new MessageSerializationException(cause);
    }
  }
  
  private static void serializeProposal(Kryo kryo, Output out, Proposal proposal) {
    KryoUtils.writeStringArray(out, proposal.getCohorts());
    out.writeVarInt(proposal.getTtl(), true);
    kryo.writeClassAndObject(out, proposal.getObjective());
  }
  
  static final class MessageDeserializationException extends KryoException {
    private static final long serialVersionUID = 1L;
    
    MessageDeserializationException(Throwable cause) {
      super("Error deserializing message", cause);
    }
  }

  @Override
  public Message read(Kryo kryo, Input in, Class<Message> type) {
    final byte messageTypeOrdinal = in.readByte();
    final MessageType messageType = MessageType.values()[messageTypeOrdinal];
    final String ballotId = in.readString();
    final long timestamp = in.readLong();
    final String source = in.readString();

    switch (messageType) {
      case PROPOSAL:
        return deserializeProposal(kryo, in, ballotId, timestamp, source);
        
//      case VOTE:
//        return deserializeVote(p, root, ballotId, timestamp, source);
//        
//      case OUTCOME:
//        return deserializeOutcome(p, root, ballotId, timestamp, source);
        
      case $UNKNOWN:
      default:
        final Throwable cause = new UnsupportedOperationException("Cannot deserialize message of type " + messageType);
        throw new MessageDeserializationException(cause);
    }
  }
  
  private static Proposal deserializeProposal(Kryo kryo, Input in, String ballotId, long timestamp, String source) {
    final String[] cohorts = KryoUtils.readStringArray(in);
    final int ttl = in.readVarInt(true);
    final Object objective = kryo.readClassAndObject(in);
    return new Proposal(ballotId, timestamp, cohorts, objective, ttl).withSource(source);
  }
}
