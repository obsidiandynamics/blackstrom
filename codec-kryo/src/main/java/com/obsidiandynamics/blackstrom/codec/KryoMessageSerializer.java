package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.model.*;

final class KryoMessageSerializer extends Serializer<Message> {
  private static final int DEF_PAYLOAD_BUFFER_SIZE = 128;
  
  static final class MessageSerializationException extends KryoException {
    private static final long serialVersionUID = 1L;
    
    MessageSerializationException(Throwable cause) {
      super("Error serializing message", cause);
    }
  }
  
  static final class MessageDeserializationException extends KryoException {
    private static final long serialVersionUID = 1L;
    
    MessageDeserializationException(Throwable cause) {
      super("Error deserializing message", cause);
    }
  }
  
  private final boolean mapPayload;
  
  KryoMessageSerializer(boolean mapPayload) {
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
        
      case VOTE:
        serializeVote(kryo, out, (Vote) message);
        break;
        
      case OUTCOME:
        serializeOutcome(kryo, out, (Outcome) message);
        break;
        
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
  
  private static void serializeVote(Kryo kryo, Output out, Vote vote) {
    serializeResponse(kryo, out, vote.getResponse());
  }
  
  private static void serializeOutcome(Kryo kryo, Output out, Outcome outcome) {
    out.writeByte(outcome.getResolution().ordinal());
    final AbortReason abortReason = outcome.getAbortReason();
    out.writeByte(abortReason != null ? abortReason.ordinal() : -1);
    final Response[] responses = outcome.getResponses();
    out.writeVarInt(responses.length, true);
    for (Response response : responses) {
      serializeResponse(kryo, out, response);
    }
  }
  
  private static void serializeResponse(Kryo kryo, Output out, Response response) {
    out.writeString(response.getCohort());
    out.writeByte(response.getIntent().ordinal());
    serializePayload(kryo, out, response.getMetadata());
  }
  
  private static void serializePayload(Kryo kryo, Output out, Object payload) {
    if (payload == null) {
      out.writeVarInt(0, true);
    } else if (payload instanceof PayloadBuffer) {
      final byte[] payloadBytes = ((PayloadBuffer) payload).getBytes();
      out.writeVarInt(payloadBytes.length, true);
      out.writeBytes(payloadBytes);
    } else {
      final Output buffer = new Output(DEF_PAYLOAD_BUFFER_SIZE, -1);
      kryo.writeClassAndObject(buffer, payload);
      final int bufferSize = buffer.position();
      out.writeVarInt(bufferSize, true);
      out.writeBytes(buffer.getBuffer(), 0, bufferSize);
    }
  }

  @Override
  public Message read(Kryo kryo, Input in, Class<Message> type) {
    final byte messageTypeOrdinal = in.readByte();
    final MessageType messageType = MessageType.values()[messageTypeOrdinal];
    final String ballotId = in.readString();
    final long timestamp = in.readLong();
    final String source = in.readString();
    final Message message;
    
    switch (messageType) {
      case PROPOSAL:
        message = deserializeProposal(kryo, in, ballotId, timestamp);
        break;
        
      case VOTE:
        message = deserializeVote(kryo, in, ballotId, timestamp);
        break;
        
      case OUTCOME:
        message = deserializeOutcome(kryo, in, ballotId, timestamp);
        break;
        
      case $UNKNOWN:
      default:
        final Throwable cause = new UnsupportedOperationException("Cannot deserialize message of type " + messageType);
        throw new MessageDeserializationException(cause);
    }
    
    message.setSource(source);
    return message;
  }
  
  private Proposal deserializeProposal(Kryo kryo, Input in, String ballotId, long timestamp) {
    final String[] cohorts = KryoUtils.readStringArray(in);
    final int ttl = in.readVarInt(true);
    final Object objective = deserializePayload(kryo, in);
    return new Proposal(ballotId, timestamp, cohorts, objective, ttl);
  }
  
  private Vote deserializeVote(Kryo kryo, Input in, String ballotId, long timestamp) {
    final Response response = deserializeResponse(kryo, in);
    return new Vote(ballotId, timestamp, response);
  }
  
  private Outcome deserializeOutcome(Kryo kryo, Input in, String ballotId, long timestamp) {
    final byte resolutionOrdinal = in.readByte();
    final Resolution resolution = Resolution.values()[resolutionOrdinal];
    final byte abortReasonOrdinal = in.readByte();
    final AbortReason abortReason = abortReasonOrdinal != -1 ? AbortReason.values()[abortReasonOrdinal] : null;
    final int responsesLength = in.readVarInt(true);
    final Response[] responses = new Response[responsesLength];
    for (int i = 0; i < responsesLength; i++) {
      responses[i] = deserializeResponse(kryo, in);
    }
    return new Outcome(ballotId, timestamp, resolution, abortReason, responses);
  }
  
  private Response deserializeResponse(Kryo kryo, Input in) {
    final String cohort = in.readString();
    final byte intentOrdinal = in.readByte();
    final Intent intent = Intent.values()[intentOrdinal];
    final Object metadata = deserializePayload(kryo, in);
    return new Response(cohort, intent, metadata);
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
