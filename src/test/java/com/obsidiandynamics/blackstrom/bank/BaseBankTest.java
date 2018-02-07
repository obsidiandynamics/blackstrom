package com.obsidiandynamics.blackstrom.bank;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;

public abstract class BaseBankTest {  
  protected static final String[] TWO_BRANCH_IDS = BankBranch.generateIds(2);
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
}
