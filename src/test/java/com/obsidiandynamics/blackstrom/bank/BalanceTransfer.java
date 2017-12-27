package com.obsidiandynamics.blackstrom.bank;

public final class BalanceTransfer {
  private final String branchId;
  
  private final int amount;

  public BalanceTransfer(String branchId, int amount) {
    this.branchId = branchId;
    this.amount = amount;
  }
  
  public String getBranchId() {
    return branchId;
  }

  public int getAmount() {
    return amount;
  }

  @Override
  public String toString() {
    return "BalanceChange [branchId=" + branchId + ", amount=" + amount + "]";
  }
}
