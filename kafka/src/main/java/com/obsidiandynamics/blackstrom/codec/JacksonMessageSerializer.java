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
    gen.writeStringField("ballotId", m.getBallotId().toString());
    gen.writeNumberField("timestamp", m.getTimestamp());
    writeString("source", m.getSource(), gen);
    
    switch (m.getMessageType()) {
      case NOMINATION:
        serializeNomination((Nomination) m, gen);
        break;
        
      case VOTE:
        break;
        
      case OUTCOME:
        break;
        
      default:
        throw new UnsupportedOperationException("Cannot serialize message of type " + m.getMessageType());
    }
    
    gen.writeEndObject();
  }
  
  private Object packConditional(Object value) {
    return value instanceof LinkedHashMap ? value : Payload.pack(value);
  }

  void serializeNomination(Nomination n, JsonGenerator gen) throws IOException {
    gen.writeArrayFieldStart("cohorts"); 
    for (String cohort : n.getCohorts()) {
      gen.writeString(cohort);
    }
    gen.writeEndArray();
    
    gen.writeNumberField("ttl", n.getTtl());
    
    JacksonUtils.writeObject("proposal", packConditional(n.getProposal()), gen);
  }
}
