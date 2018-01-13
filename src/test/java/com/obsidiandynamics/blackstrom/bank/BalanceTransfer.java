package com.obsidiandynamics.blackstrom.bank;

public final class BalanceTransfer {
  private final String branchId;
  
  private final long amount;

  public BalanceTransfer(String branchId, long amount) {
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
