package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.fasterxml.jackson.databind.node.*;
import com.obsidiandynamics.blackstrom.model.*;

final class JacksonMessageDeserializer extends StdDeserializer<Message> {
  private static final long serialVersionUID = 1L;
  
  private final boolean mapPayload;
  
  JacksonMessageDeserializer(boolean mapPayload) {
    super(Message.class);
    this.mapPayload = mapPayload;
  }
  
  private Class<?> getPayloadClass() {
    return mapPayload ? Payload.class : Object.class;
  }
  
  static final class MessageDeserializationException extends JsonProcessingException {
    private static final long serialVersionUID = 1L;
    
    MessageDeserializationException(Throwable cause) {
      super("Error deserializing message", cause);
    }
  }
  
  @Override
  public Message deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    final JsonNode root = p.getCodec().readTree(p);
    final MessageType messageType = MessageType.valueOf(root.get("messageType").asText());
    final JsonNode xidNode = root.get("xid");
    final String xid = xidNode.asText();
    final long timestamp = root.get("timestamp").asLong();
    final String source = JacksonUtils.readString("source", root);
    final Message message;
    
    switch (messageType) {
      case QUERY:
        message = deserializeQuery(p, root, xid, timestamp);
        break;

      case QUERY_RESPONSE:
        message = deserializeQueryResponse(p, root, xid, timestamp);
        break;

      case COMMAND:
        message = deserializeCommand(p, root, xid, timestamp);
        break;

      case COMMAND_RESPONSE:
        message = deserializeCommandResponse(p, root, xid, timestamp);
        break;

      case NOTICE:
        message = deserializeNotice(p, root, xid, timestamp);
        break;
      
      case PROPOSAL:
        message = deserializeProposal(p, root, xid, timestamp);
        break;
        
      case VOTE:
        message = deserializeVote(p, root, xid, timestamp);
        break;
        
      case OUTCOME:
        message = deserializeOutcome(p, root, xid, timestamp);
        break;
        
      case $UNKNOWN:
      default:
        final Throwable cause = new UnsupportedOperationException("Cannot deserialize message of type " + messageType);
        throw new MessageDeserializationException(cause);
    }
    
    message.setSource(source);
    return message;
  }
  
  private Message deserializeQuery(JsonParser p, JsonNode root, String xid, long timestamp) throws JsonProcessingException {
    final int ttl = root.get("ttl").asInt();
    final Object objective = Payload.unpack(JacksonUtils.readObject("objective", root, p, getPayloadClass()));
    return new Query(xid, timestamp, objective, ttl);
  }
  
  private Message deserializeQueryResponse(JsonParser p, JsonNode root, String xid, long timestamp) throws JsonProcessingException {
    final Object reply = Payload.unpack(JacksonUtils.readObject("result", root, p, getPayloadClass()));
    return new QueryResponse(xid, timestamp, reply);
  }
  
  private Message deserializeCommand(JsonParser p, JsonNode root, String xid, long timestamp) throws JsonProcessingException {
    final int ttl = root.get("ttl").asInt();
    final Object objective = Payload.unpack(JacksonUtils.readObject("objective", root, p, getPayloadClass()));
    return new Command(xid, timestamp, objective, ttl);
  }
  
  private Message deserializeCommandResponse(JsonParser p, JsonNode root, String xid, long timestamp) throws JsonProcessingException {
    final Object reply = Payload.unpack(JacksonUtils.readObject("result", root, p, getPayloadClass()));
    return new CommandResponse(xid, timestamp, reply);
  }
  
  private Message deserializeNotice(JsonParser p, JsonNode root, String xid, long timestamp) throws JsonProcessingException {
    final Object event = Payload.unpack(JacksonUtils.readObject("event", root, p, getPayloadClass()));
    return new Notice(xid, timestamp, event);
  }
  
  private Message deserializeProposal(JsonParser p, JsonNode root, String xid, long timestamp) throws JsonProcessingException {
    final ArrayNode cohortsNode = (ArrayNode) root.get("cohorts");
    final String[] cohorts = new String[cohortsNode.size()];
    for (int i = 0; i < cohorts.length; i++) {
      cohorts[i] = cohortsNode.get(i).asText();
    }
    
    final int ttl = root.get("ttl").asInt();
    final Object objective = Payload.unpack(JacksonUtils.readObject("objective", root, p, getPayloadClass()));
    return new Proposal(xid, timestamp, cohorts, objective, ttl);
  }
  
  private Message deserializeVote(JsonParser p, JsonNode root, String xid, long timestamp) throws JsonProcessingException {
    final JsonNode responseNode = root.get("response");
    final Response response = deserializeResponse(p, responseNode);
    return new Vote(xid, timestamp, response);
  }
  
  private Response deserializeResponse(JsonParser p, JsonNode responseNode) throws JsonProcessingException {
    final String cohort = responseNode.get("cohort").asText();
    final Intent intent = Intent.valueOf(responseNode.get("intent").asText());
    final Object metadata = Payload.unpack(JacksonUtils.readObject("metadata", responseNode, p, getPayloadClass()));
    return new Response(cohort, intent, metadata);
  }
  
  private Message deserializeOutcome(JsonParser p, JsonNode root, String xid, long timestamp) throws JsonProcessingException {
    final Resolution resolution = Resolution.valueOf(root.get("resolution").asText());
    final String abortReasonStr = JacksonUtils.readString("abortReason", root);
    final AbortReason abortReason = abortReasonStr != null ? AbortReason.valueOf(abortReasonStr) : null;
    final ArrayNode responsesNode = (ArrayNode) root.get("responses");
    final Response[] responses = new Response[responsesNode.size()];
    for (int i = 0; i < responses.length; i++) {
      responses[i] = deserializeResponse(p, responsesNode.get(i));
    }
    final Object metadata = Payload.unpack(JacksonUtils.readObject("metadata", root, p, getPayloadClass()));
    return new Outcome(xid, timestamp, resolution, abortReason, responses, metadata);
  }
}
