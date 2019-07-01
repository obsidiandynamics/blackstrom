package com.obsidiandynamics.blackstrom.bank;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public abstract class BaseBankTest {  
  protected static final String[] TWO_BRANCH_IDS = BankBranch.generateIds(2);
  protected static final int PROPOSAL_TIMEOUT_MILLIS = 300_000; //TODO (was 30_000)
  protected static final int FUTURE_GET_TIMEOUT_MILLIS = PROPOSAL_TIMEOUT_MILLIS * 2;
  
  protected Ledger ledger;
  
  protected final Timesert wait = getWait();

  protected Manifold manifold;
  
  protected abstract Ledger createLedger(Guidance guidance);
  
  protected abstract Timesert getWait();
  
  @After
  public final void afterBase() {
    if (manifold != null) {
      manifold.dispose();
      manifold = null;
    }
  }

  protected final void buildCoordinatedManifold(MonitorEngineConfig engineConfig, Initiator initiator, Factor... branches) {
    ledger = createLedger(Guidance.COORDINATED);
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(initiator, new DefaultMonitor(engineConfig))
        .withFactors(branches)
        .build();
  }
  
  protected final void buildAutonomousManifold(MonitorEngineConfig engineConfig, Initiator initiator, Factor... branches) {
    ledger = createLedger(Guidance.AUTONOMOUS);
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(new InlineMonitor(engineConfig, initiator))
        .withFactors(Arrays.stream(branches)
                     .map(f -> new InlineMonitor(engineConfig, f))
                     .collect(Collectors.toList()))
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
