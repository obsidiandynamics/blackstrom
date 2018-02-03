package com.obsidiandynamics.blackstrom.codec;

import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.codec.JacksonMessageCodec.*;

public final class JacksonBankExpansion implements JacksonExpansion {
  @Override
  public void accept(SimpleModule module) {
    module.addSerializer(BankSettlement.class, new JacksonBankSettlementSerializer());
    module.addDeserializer(BankSettlement.class, new JacksonBankSettlementDeserializer());
  }
}
