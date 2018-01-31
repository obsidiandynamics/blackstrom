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
    serializePayload(kryo, out, proposal.getObjective());
  }
  
  private static void serializePayload(Kryo kryo, Output out, Object payload) {
    if (payload == null) {
      out.writeVarInt(0, true);
    } else if (payload instanceof PayloadBuffer) {
      final byte[] payloadBytes = ((PayloadBuffer) payload).getBytes();
      out.writeVarInt(payloadBytes.length, true);
      out.writeBytes(payloadBytes);
    } else {
      final Output buffer = new Output(64, -1);
      kryo.writeClassAndObject(buffer, payload);
      final int bufferSize = buffer.position();
      out.writeVarInt(bufferSize, true);
      out.writeBytes(buffer.getBuffer(), 0, bufferSize);
    }
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
  
  private Proposal deserializeProposal(Kryo kryo, Input in, String ballotId, long timestamp, String source) {
    final String[] cohorts = KryoUtils.readStringArray(in);
    final int ttl = in.readVarInt(true);
    final Object objective = deserializePayload(kryo, in);
    return new Proposal(ballotId, timestamp, cohorts, objective, ttl).withSource(source);
  }
  
  private Object deserializePayload(Kryo kryo, Input in) {
    final int bufferSize = in.readVarInt(true);
    if (bufferSize != 0) {
      final byte[] buffer = new byte[bufferSize];
      in.readBytes(buffer);
      if (mapPayload) {
        return kryo.readClassAndObject(new Input(buffer));
      } else {
        return new PayloadBuffer(buffer);
      }
    } else {
      return null;
    }
  }
}
