package com.obsidiandynamics.blackstrom.bank;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.util.*;

public abstract class BaseBankTest {  
  protected static final String[] TWO_BRANCH_IDS = new String[] { getBranchId(0), getBranchId(1) };
  protected static final int TWO_BRANCHES = TWO_BRANCH_IDS.length;
  protected static final int PROPOSAL_TIMEOUT = 30_000;
  protected static final int FUTURE_GET_TIMEOUT = PROPOSAL_TIMEOUT * 2;
  
  protected Ledger ledger;
  
  protected final Timesert wait = getWait();

  protected Manifold manifold;
  
  protected abstract Ledger createLedger();
  
  protected abstract Timesert getWait();
  
  @After
  public final void after() {
    if (manifold != null) manifold.dispose();
  }
  
  protected final void buildStandardManifold(Factor initiator, Factor monitor, Factor... branches) {
    ledger = createLedger();
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(initiator, monitor)
        .withFactors(branches)
        .build();
  }

  protected static final long getTotalBalance(BankBranch[] branches) {
    return Arrays.stream(branches).collect(Collectors.summarizingLong(b -> b.getBalance())).getSum();
  }

  protected static final boolean allZeroEscrow(BankBranch[] branches) {
    return Arrays.stream(branches).allMatch(b -> b.getEscrow() == 0);
  }

  protected static final boolean nonZeroBalances(BankBranch[] branches) {
    return Arrays.stream(branches).allMatch(b -> b.getBalance() >= 0);
  }

  protected static final BankSettlement generateRandomSettlement(String[] branchIds, long amount) {
    final Map<String, BalanceTransfer> transfers = new HashMap<>(branchIds.length);
    long sum = 0;
    for (int i = 0; i < branchIds.length - 1; i++) {
      final long randomAmount = amount - (long) (Math.random() * amount * 2);
      sum += randomAmount;
      final String branchId = branchIds[i];
      transfers.put(branchId, new BalanceTransfer(branchId, randomAmount));
    }
    final String lastBranchId = branchIds[branchIds.length - 1];
    transfers.put(lastBranchId, new BalanceTransfer(lastBranchId, -sum));
    return new BankSettlement(transfers);
  }

  protected static final String[] generateBranches(int numBranches) {
    final String[] branches = new String[numBranches];
    for (int i = 0; i < numBranches; i++) {
      branches[i] = getBranchId(i);
    }
    return branches;
  }

  protected static final String getBranchId(int branchIdx) {
    return "branch-" + branchIdx;
  }

  protected static final BankBranch[] createBranches(int numBranches, long initialBalance, boolean idempotencyEnabled, Sandbox sandbox) {
    final BankBranch[] branches = new BankBranch[numBranches];
    for (int branchIdx = 0; branchIdx < numBranches; branchIdx++) {
      branches[branchIdx] = new BankBranch(getBranchId(branchIdx), initialBalance, idempotencyEnabled, sandbox::contains);
    }
    return branches;
  }
}
