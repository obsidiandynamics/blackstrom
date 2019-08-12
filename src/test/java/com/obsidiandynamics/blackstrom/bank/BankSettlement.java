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
      return withTransfers(List.of(transfers));
    }

    public BankSettlementBuilder withTransfers(List<BalanceTransfer> transfers) {
      this.transfers.addAll(transfers);
      return this;
    }

    public BankSettlement build() {
      return new BankSettlement(transfers.stream().collect(Collectors.toMap(t -> t.getBranchId(), Function.identity())));
    }
  }

  public static final BankSettlement randomise(String[] branchIds, long maxAbsoluteAmount) {
    final Map<String, BalanceTransfer> transfers = new HashMap<>(branchIds.length, 1f);
    long sum = 0;
    for (int i = 0; i < branchIds.length - 1; i++) {
      final long randomAmount = maxAbsoluteAmount - (long) (Math.random() * maxAbsoluteAmount * 2);
      sum += randomAmount;
      final String branchId = branchIds[i];
      transfers.put(branchId, new BalanceTransfer(branchId, randomAmount));
    }
    final String lastBranchId = branchIds[branchIds.length - 1];
    transfers.put(lastBranchId, new BalanceTransfer(lastBranchId, -sum));
    return new BankSettlement(transfers);
  }
}
