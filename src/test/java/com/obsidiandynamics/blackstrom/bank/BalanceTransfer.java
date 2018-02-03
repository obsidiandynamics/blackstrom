package com.obsidiandynamics.blackstrom.bank;

import org.apache.commons.lang3.builder.*;

public final class BalanceTransfer {
  private final String branchId;
  
  private final long amount;

  public BalanceTransfer(String branchId, long amount) {
    this.branchId = branchId;
    this.amount = amount;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(branchId).append(amount).toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof BalanceTransfer) {
      final BalanceTransfer that = (BalanceTransfer) obj;
      return new EqualsBuilder()
          .append(branchId, that.branchId)
          .append(amount, that.amount)
          .isEquals();
    } else {
      return false;
    }
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
