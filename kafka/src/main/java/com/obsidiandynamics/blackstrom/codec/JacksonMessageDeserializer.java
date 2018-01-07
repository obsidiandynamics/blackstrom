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
  
  @Override
  public Message deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    final JsonNode root = p.getCodec().readTree(p);
    final MessageType messageType = MessageType.valueOf(root.get("messageType").asText());
    final String ballotId = root.get("ballotId").asText();
    final long timestamp = root.get("timestamp").asLong();
    final String source = JacksonUtils.readString("source", root);
    
    switch (messageType) {
      case NOMINATION:
        return deserializeNomination(p, root, ballotId, timestamp, source);
        
      default:
        throw new UnsupportedOperationException("Cannot serialize message of type " + messageType);
    }
  }
  
  private Message deserializeNomination(JsonParser p, JsonNode root, String ballotId, long timestamp, String source) throws JsonProcessingException {
    final ArrayNode cohortsNode = (ArrayNode) root.get("cohorts");
    final String[] cohorts = new String[cohortsNode.size()];
    for (int i = 0; i < cohorts.length; i++) {
      cohorts[i] = cohortsNode.get(i).asText();
    }
    
    final int ttl = root.get("ttl").asInt();
    final Object proposal = Payload.unpack(JacksonUtils.readObject("proposal", root, p, getPayloadClass()));
    return new Nomination(ballotId, timestamp, cohorts, proposal, ttl).withSource(source);
  }
}
