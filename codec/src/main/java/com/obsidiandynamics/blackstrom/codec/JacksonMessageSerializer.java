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
    final Object ballotId = m.getBallotId();
    if (ballotId instanceof Number) {
      gen.writeNumberField("ballotId", ((Number) ballotId).longValue());
    } else {
      gen.writeStringField("ballotId", m.getBallotId().toString());
    }
    gen.writeNumberField("timestamp", m.getTimestamp());
    writeString("source", m.getSource(), gen);
    
    switch (m.getMessageType()) {
      case NOMINATION:
        serializeNomination((Nomination) m, gen);
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

  private void serializeNomination(Nomination n, JsonGenerator gen) throws IOException {
    gen.writeArrayFieldStart("cohorts"); 
    for (String cohort : n.getCohorts()) {
      gen.writeString(cohort);
    }
    gen.writeEndArray();
    gen.writeNumberField("ttl", n.getTtl());
    JacksonUtils.writeObject("proposal", packConditional(n.getProposal()), gen);
  }
  
  private void serializeVote(Vote v, JsonGenerator gen) throws IOException {
    gen.writeFieldName("response");
    serializeResponse(v.getResponse(), gen);
  }
  
  private void serializeResponse(Response r, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("cohort", r.getCohort());
    gen.writeStringField("pledge", r.getPledge().name());
    JacksonUtils.writeObject("metadata", packConditional(r.getMetadata()), gen);
    gen.writeEndObject();
  }
  
  private void serializeOutcome(Outcome o, JsonGenerator gen) throws IOException {
    gen.writeStringField("verdict", o.getVerdict().name());
    final AbortReason abortReason = o.getAbortReason();
    if (abortReason != null) {
      gen.writeStringField("abortReason", abortReason.name());      
    }
    gen.writeFieldName("responses");
    gen.writeStartArray();
    for (Response response : o.getResponses()) {
      serializeResponse(response, gen);
    }
    gen.writeEndArray();
  }
}
