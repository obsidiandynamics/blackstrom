package com.obsidiandynamics.blackstrom.bank;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;

public abstract class AbstractBankTransferFailureTest extends BaseBankTest {  
  @Test
  public final void testFactorFailures() {
    final RxTxFailureModes[] presetFailureModesArray = new RxTxFailureModes[] {
      new RxTxFailureModes() {},
      new RxTxFailureModes() {{
        rxFailureMode = new DuplicateDelivery(1);
      }},
      new RxTxFailureModes() {{
        rxFailureMode = new DelayedDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        rxFailureMode = new DelayedDuplicateDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DuplicateDelivery(1);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DelayedDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DelayedDuplicateDelivery(1, 10);
      }}
    };
    
    for (TargetFactor target : TargetFactor.values()) {
      for (RxTxFailureModes failureModes : presetFailureModesArray) {
        boolean success = false;
        try {
          testFactorFailure(new FailureModes().set(target, failureModes));
          success = true;
        } catch (Exception e) {
          throw new AssertionError(String.format("target=%s, failureModes=%s", target, failureModes), e);
        } finally {
          if (! success) System.out.format("Failure for target=%s, failureModes=%s\n", target, failureModes);
          manifold.dispose();
        }
      }
    }
  }
  
  private enum TargetFactor {
    INITIATOR,
    COHORT,
    MONITOR
  }
  
  private abstract static class RxTxFailureModes {
    FailureMode rxFailureMode; 
    FailureMode txFailureMode;
    
    @Override
    public String toString() {
      return RxTxFailureModes.class.getSimpleName() + " [rxFailureMode=" + rxFailureMode + 
          ", txFailureMode=" + txFailureMode + "]";
    }
  }
  
  private static class FailureModes extends EnumMap<TargetFactor, RxTxFailureModes> {
    private static final long serialVersionUID = 1L;
    
    FailureModes() {
      super(TargetFactor.class);
      for (TargetFactor target : TargetFactor.values()) {
        set(target, new RxTxFailureModes() {});
      }
    }
    
    FailureModes set(TargetFactor target, RxTxFailureModes rxtxFailureModes) {
      put(target, rxtxFailureModes);
      return this;
    }
  }

  private void testFactorFailure(Map<TargetFactor, RxTxFailureModes> failureModes) throws InterruptedException, ExecutionException, Exception {
    final int initialBalance = 1_000;
    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final Sandbox sandbox = Sandbox.forTest(this);
    final BankBranch[] branches = createBranches(2, initialBalance, true, sandbox);

    ledger = createLedger();
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(new FallibleFactor(initiator)
                     .withRxFailureMode(failureModes.get(TargetFactor.INITIATOR).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.INITIATOR).txFailureMode),
                     new FallibleFactor(monitor)
                     .withRxFailureMode(failureModes.get(TargetFactor.MONITOR).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.MONITOR).txFailureMode),
                     new FallibleFactor(branches[0])
                     .withRxFailureMode(failureModes.get(TargetFactor.COHORT).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.COHORT).txFailureMode),
                     branches[1])
        .build();

    testSingleTransfer(initialBalance + 1, Verdict.ABORT, AbortReason.REJECT, initiator, sandbox);
    testSingleTransfer(initialBalance, Verdict.COMMIT, null, initiator, sandbox);

    wait.until(() -> {
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
      assertTrue("branches=" + Arrays.asList(branches), nonZeroBalances(branches));
    });
  }

  private void testSingleTransfer(int transferAmount, Verdict expectedVerdict, AbortReason expectedAbortReason,
                                  AsyncInitiator initiator, Sandbox sandbox) throws InterruptedException, ExecutionException, Exception {
    assert expectedVerdict == Verdict.COMMIT ^ expectedAbortReason != null;
    final String ballotId = UUID.randomUUID().toString();
    final Outcome o = initiator.initiate(new Proposal(ballotId, 
                                                      TWO_BRANCH_IDS, 
                                                      BankSettlement.builder()
                                                      .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                                     new BalanceTransfer(getBranchId(1), transferAmount))
                                                      .build(),
                                                      PROPOSAL_TIMEOUT).withShardKey(sandbox.key()))
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(expectedVerdict, o.getVerdict());
    assertEquals(expectedAbortReason, o.getAbortReason());
  }
}
