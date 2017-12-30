package com.obsidiandynamics.blackstrom.bank;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

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
    return "BankSettlement [xfers=" + transfers.values() + "]";
  }
  
  public static BankSettlementBuilder builder() {
    return new BankSettlementBuilder();
  }
  
  public static final class BankSettlementBuilder {
    private final List<BalanceTransfer> transfers = new ArrayList<>();
    
    public BankSettlementBuilder withTransfers(BalanceTransfer... transfers) {
      return withTransfers(Arrays.asList(transfers));
    }
    
    public BankSettlementBuilder withTransfers(List<BalanceTransfer> transfers) {
      this.transfers.addAll(transfers);
      return this;
    }
    
    public BankSettlement build() {
      return new BankSettlement(transfers.stream().collect(Collectors.toMap(t -> t.getBranchId(), Function.identity())));
    }
  }
}
