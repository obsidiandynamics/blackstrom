package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.blackstrom.codec.JacksonUtils.*;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.*;
import com.obsidiandynamics.blackstrom.model.*;

final class JacksonMessageSerializer extends StdSerializer<Message> {
  private static final long serialVersionUID = 1L;

  JacksonMessageSerializer() {
    super(Message.class);
  }

  @Override
  public void serialize(Message m, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("messageType", m.getMessageType().name());
    gen.writeStringField("xid", m.getXid());
    gen.writeNumberField("timestamp", m.getTimestamp());
    writeString("source", m.getSource(), gen);
    
    switch (m.getMessageType()) {
      case QUERY:
        serializeQuery((Query) m, gen);
        break;

      case QUERY_RESPONSE:
        serializeQueryResponse((QueryResponse) m, gen);
        break;
        
      case COMMAND:
        serializeCommand((Command) m, gen);
        break;

      case COMMAND_RESPONSE:
        serializeCommandResponse((CommandResponse) m, gen);
        break;
        
      case NOTICE:
        serializeNotice((Notice) m, gen);
        break;

      case PROPOSAL:
        serializeProposal((Proposal) m, gen);
        break;
        
      case VOTE:
        serializeVote((Vote) m, gen);
        break;
        
      case OUTCOME:
        serializeOutcome((Outcome) m, gen);
        break;
        
      case $UNKNOWN:
      default:
        throw new UnsupportedOperationException("Cannot serialize message of type " + m.getMessageType());
    }
    
    gen.writeEndObject();
  }
  
  private Object packConditional(Object value) {
    return value instanceof LinkedHashMap ? value : Payload.pack(value);
  }
  
  private void serializeQuery(Query m, JsonGenerator gen) throws IOException {
    gen.writeNumberField("ttl", m.getTtl());
    JacksonUtils.writeObject("objective", packConditional(m.getObjective()), gen);
  }
  
  private void serializeQueryResponse(QueryResponse m, JsonGenerator gen) throws IOException {
    JacksonUtils.writeObject("result", packConditional(m.getResult()), gen);
  }
  
  private void serializeCommand(Command m, JsonGenerator gen) throws IOException {
    gen.writeNumberField("ttl", m.getTtl());
    JacksonUtils.writeObject("objective", packConditional(m.getObjective()), gen);
  }
  
  private void serializeCommandResponse(CommandResponse m, JsonGenerator gen) throws IOException {
    JacksonUtils.writeObject("result", packConditional(m.getResult()), gen);
  }
  
  private void serializeNotice(Notice m, JsonGenerator gen) throws IOException {
    JacksonUtils.writeObject("event", packConditional(m.getEvent()), gen);
  }

  private void serializeProposal(Proposal m, JsonGenerator gen) throws IOException {
    gen.writeArrayFieldStart("cohorts"); 
    for (String cohort : m.getCohorts()) {
      gen.writeString(cohort);
    }
    gen.writeEndArray();
    gen.writeNumberField("ttl", m.getTtl());
    JacksonUtils.writeObject("objective", packConditional(m.getObjective()), gen);
  }
  
  private void serializeVote(Vote m, JsonGenerator gen) throws IOException {
    gen.writeFieldName("response");
    serializeResponse(m.getResponse(), gen);
  }
  
  private void serializeResponse(Response r, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("cohort", r.getCohort());
    gen.writeStringField("intent", r.getIntent().name());
    JacksonUtils.writeObject("metadata", packConditional(r.getMetadata()), gen);
    gen.writeEndObject();
  }
  
  private void serializeOutcome(Outcome m, JsonGenerator gen) throws IOException {
    gen.writeStringField("resolution", m.getResolution().name());
    final AbortReason abortReason = m.getAbortReason();
    if (abortReason != null) {
      gen.writeStringField("abortReason", abortReason.name());      
    }
    gen.writeFieldName("responses");
    gen.writeStartArray();
    for (Response response : m.getResponses()) {
      serializeResponse(response, gen);
    }
    gen.writeEndArray();
    JacksonUtils.writeObject("metadata", packConditional(m.getMetadata()), gen);
  }
}
