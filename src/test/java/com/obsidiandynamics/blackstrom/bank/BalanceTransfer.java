package com.obsidiandynamics.blackstrom.bank;

import com.fasterxml.jackson.annotation.*;

public final class BalanceTransfer {
  private final String branchId;
  
  private final long amount;

  @JsonCreator
  public BalanceTransfer(@JsonProperty(value="branchId") String branchId, 
                         @JsonProperty(value="amount") long amount) {
    this.branchId = branchId;
    this.amount = amount;
  }
  
  public String getBranchId() {
    return branchId;
  }

  public long getAmount() {
    return amount;
  }

  @Override
  public String toString() {
    return BalanceTransfer.class.getSimpleName() + " [branchId=" + branchId + ", amount=" + amount + "]";
  }
}
