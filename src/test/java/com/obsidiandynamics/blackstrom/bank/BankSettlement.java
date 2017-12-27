package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

public final class BankSettlement {
  private final Map<String, BalanceTransfer> transfers;

  public BankSettlement(Map<String, BalanceTransfer> transfers) {
    this.transfers = transfers;
  }

  public Map<String, BalanceTransfer> getTransfers() {
    return transfers;
  }

  @Override
  public String toString() {
    return "BankSettlement [transfers=" + transfers.values() + "]";
  }
}
