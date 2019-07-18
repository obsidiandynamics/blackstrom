package com.obsidiandynamics.blackstrom.codec;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.obsidiandynamics.blackstrom.bank.*;

public final class JacksonBankSettlementDeserializer extends StdDeserializer<BankSettlement> {
  private static final long serialVersionUID = 1L;
  
  JacksonBankSettlementDeserializer() {
    super(BankSettlement.class);
  }
  
  @Override
  public BankSettlement deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    final JsonNode root = p.getCodec().readTree(p);
    final JsonNode transfersNode = root.get("transfers");
    final int transfersLength = transfersNode.size();
    final Map<String, BalanceTransfer> transfers = new HashMap<>(transfersLength, 1f);
    for (Iterator<Map.Entry<String, JsonNode>> fieldIt = transfersNode.fields(); fieldIt.hasNext();) {
      final Map.Entry<String, JsonNode> field = fieldIt.next();
      final String branchId = field.getKey();
      final JsonNode fieldNode = field.getValue();
      final long amount = fieldNode.get("amount").asLong();
      final BalanceTransfer xfer = new BalanceTransfer(branchId, amount);
      transfers.put(branchId, xfer);
    }
    return new BankSettlement(transfers);
  }
}
