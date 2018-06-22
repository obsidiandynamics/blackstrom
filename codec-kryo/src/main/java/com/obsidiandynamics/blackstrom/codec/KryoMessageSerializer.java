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
    out.writeString(message.getXid());
    out.writeLong(message.getTimestamp());
    out.writeString(message.getSource());
    
    switch (message.getMessageType()) {
      case QUERY:
        serializeQuery(kryo, out, (Query) message);
        break;

      case QUERY_RESPONSE:
        serializeQueryResponse(kryo, out, (QueryResponse) message);
        break;

      case COMMAND:
        serializeCommand(kryo, out, (Command) message);
        break;

      case COMMAND_RESPONSE:
        serializeCommandResponse(kryo, out, (CommandResponse) message);
        break;
        
      case NOTICE:
        serializeNotice(kryo, out, (Notice) message);
        break;
      
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
  
  private static void serializeQuery(Kryo kryo, Output out, Query m) {
    out.writeVarInt(m.getTtl(), true);
    serializePayload(kryo, out, m.getObjective());
  }
  
  private static void serializeQueryResponse(Kryo kryo, Output out, QueryResponse m) {
    serializePayload(kryo, out, m.getResult());
  }
  
  private static void serializeCommand(Kryo kryo, Output out, Command m) {
    out.writeVarInt(m.getTtl(), true);
    serializePayload(kryo, out, m.getObjective());
  }
  
  private static void serializeCommandResponse(Kryo kryo, Output out, CommandResponse m) {
    serializePayload(kryo, out, m.getResult());
  }
  
  private static void serializeNotice(Kryo kryo, Output out, Notice m) {
    serializePayload(kryo, out, m.getEvent());
  }
  
  private static void serializeProposal(Kryo kryo, Output out, Proposal m) {
    KryoUtils.writeStringArray(out, m.getCohorts());
    out.writeVarInt(m.getTtl(), true);
    serializePayload(kryo, out, m.getObjective());
  }
  
  private static void serializeVote(Kryo kryo, Output out, Vote m) {
    serializeResponse(kryo, out, m.getResponse());
  }
  
  private static void serializeOutcome(Kryo kryo, Output out, Outcome m) {
    out.writeByte(m.getResolution().ordinal());
    final AbortReason abortReason = m.getAbortReason();
    out.writeByte(abortReason != null ? abortReason.ordinal() : -1);
    final Response[] responses = m.getResponses();
    out.writeVarInt(responses.length, true);
    for (Response response : responses) {
      serializeResponse(kryo, out, response);
    }
    serializePayload(kryo, out, m.getMetadata());
  }
  
  private static void serializeResponse(Kryo kryo, Output out, Response r) {
    out.writeString(r.getCohort());
    out.writeByte(r.getIntent().ordinal());
    serializePayload(kryo, out, r.getMetadata());
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
    final String xid = in.readString();
    final long timestamp = in.readLong();
    final String source = in.readString();
    final Message message;
    
    switch (messageType) {
      case QUERY:
        message = deserializeQuery(kryo, in, xid, timestamp);
        break;

      case QUERY_RESPONSE:
        message = deserializeQueryResponse(kryo, in, xid, timestamp);
        break;

      case COMMAND:
        message = deserializeCommand(kryo, in, xid, timestamp);
        break;

      case COMMAND_RESPONSE:
        message = deserializeCommandResponse(kryo, in, xid, timestamp);
        break;
        
      case NOTICE:
        message = deserializeNotice(kryo, in, xid, timestamp);
        break;
        
      case PROPOSAL:
        message = deserializeProposal(kryo, in, xid, timestamp);
        break;
        
      case VOTE:
        message = deserializeVote(kryo, in, xid, timestamp);
        break;
        
      case OUTCOME:
        message = deserializeOutcome(kryo, in, xid, timestamp);
        break;
        
      case $UNKNOWN:
      default:
        final Throwable cause = new UnsupportedOperationException("Cannot deserialize message of type " + messageType);
        throw new MessageDeserializationException(cause);
    }
    
    message.setSource(source);
    return message;
  }
  
  private Query deserializeQuery(Kryo kryo, Input in, String xid, long timestamp) {
    final int ttl = in.readVarInt(true);
    final Object objective = deserializePayload(kryo, in);
    return new Query(xid, timestamp, objective, ttl);
  }
  
  private QueryResponse deserializeQueryResponse(Kryo kryo, Input in, String xid, long timestamp) {
    final Object result = deserializePayload(kryo, in);
    return new QueryResponse(xid, timestamp, result);
  }
  
  private Command deserializeCommand(Kryo kryo, Input in, String xid, long timestamp) {
    final int ttl = in.readVarInt(true);
    final Object objective = deserializePayload(kryo, in);
    return new Command(xid, timestamp, objective, ttl);
  }
  
  private CommandResponse deserializeCommandResponse(Kryo kryo, Input in, String xid, long timestamp) {
    final Object result = deserializePayload(kryo, in);
    return new CommandResponse(xid, timestamp, result);
  }
  
  private Notice deserializeNotice(Kryo kryo, Input in, String xid, long timestamp) {
    final Object event = deserializePayload(kryo, in);
    return new Notice(xid, timestamp, event);
  }
  
  private Proposal deserializeProposal(Kryo kryo, Input in, String xid, long timestamp) {
    final String[] cohorts = KryoUtils.readStringArray(in);
    final int ttl = in.readVarInt(true);
    final Object objective = deserializePayload(kryo, in);
    return new Proposal(xid, timestamp, cohorts, objective, ttl);
  }
  
  private Vote deserializeVote(Kryo kryo, Input in, String xid, long timestamp) {
    final Response response = deserializeResponse(kryo, in);
    return new Vote(xid, timestamp, response);
  }
  
  private Outcome deserializeOutcome(Kryo kryo, Input in, String xid, long timestamp) {
    final byte resolutionOrdinal = in.readByte();
    final Resolution resolution = Resolution.values()[resolutionOrdinal];
    final byte abortReasonOrdinal = in.readByte();
    final AbortReason abortReason = abortReasonOrdinal != -1 ? AbortReason.values()[abortReasonOrdinal] : null;
    final int responsesLength = in.readVarInt(true);
    final Response[] responses = new Response[responsesLength];
    for (int i = 0; i < responsesLength; i++) {
      responses[i] = deserializeResponse(kryo, in);
    }
    final Object metadata = deserializePayload(kryo, in);
    return new Outcome(xid, timestamp, resolution, abortReason, responses, metadata);
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
