package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.*;
import com.obsidiandynamics.blackstrom.bank.*;

public class JacksonBankSettlementSerializer extends StdSerializer<BankSettlement> {
  private static final long serialVersionUID = 1L;

  JacksonBankSettlementSerializer() {
    super(BankSettlement.class);
  }

  @Override
  public void serialize(BankSettlement settlement, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeObjectFieldStart("transfers"); 
    for (BalanceTransfer xfer : settlement.getTransfers().values()) {
      gen.writeObjectFieldStart(xfer.getBranchId());
      gen.writeNumberField("amount", xfer.getAmount());
      gen.writeEndObject();
    }
    gen.writeEndObject();
    gen.writeEndObject();
  }
}
