package com.obsidiandynamics.blackstrom.codec;

import java.util.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.bank.*;

public final class KryoBankSettlementSerializer extends Serializer<BankSettlement> {
  @Override
  public void write(Kryo kryo, Output out, BankSettlement settlement) {
    final Map<String, BalanceTransfer> transfers = settlement.getTransfers();
    out.writeVarInt(transfers.size(), true);
    for (BalanceTransfer xfer : transfers.values()) {
      serializeBalanceTransfer(kryo, out, xfer);
    }
  }
  
  private static void serializeBalanceTransfer(Kryo kryo, Output out, BalanceTransfer xfer) {
    out.writeString(xfer.getBranchId());
    out.writeLong(xfer.getAmount());
  }

  @Override
  public BankSettlement read(Kryo kryo, Input in, Class<? extends BankSettlement> type) {
    final int transfersSize = in.readVarInt(true);
    final Map<String, BalanceTransfer> transfers = new HashMap<>(transfersSize);
    for (int i = 0; i < transfersSize; i++) {
      final BalanceTransfer xfer = deserializeTransfer(kryo, in);
      transfers.put(xfer.getBranchId(), xfer);
    }
    return new BankSettlement(transfers);
  }
  
  private static BalanceTransfer deserializeTransfer(Kryo kryo, Input in) {
    return new BalanceTransfer(in.readString(), in.readLong());
  }
}
