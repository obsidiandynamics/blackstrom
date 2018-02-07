package com.obsidiandynamics.blackstrom.bank;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import org.apache.commons.lang3.builder.*;

public final class BankSettlement {
  private final Map<String, BalanceTransfer> transfers;

  public BankSettlement(Map<String, BalanceTransfer> transfers) {
    this.transfers = transfers;
  }

  public Map<String, BalanceTransfer> getTransfers() {
    return transfers;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(transfers).toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof BankSettlement) {
      final BankSettlement that = (BankSettlement) obj;
      return new EqualsBuilder().append(transfers, that.transfers).isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return BankSettlement.class.getSimpleName() + " [xfers=" + transfers.values() + "]";
  }
  
  public static BankSettlement forTwo(long amount) {
    return builder()
        .withTransfers(new BalanceTransfer(BankBranch.getId(0), -amount), 
                       new BalanceTransfer(BankBranch.getId(1), amount))
        .build();
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
