package com.obsidiandynamics.blackstrom.bank;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.zerolog.*;

public abstract class AbstractBankTransferFailureTest extends BaseBankTest {  
  private static final Zlg zlg = Zlg.forDeclaringClass().get();

  private static final int DELIVERY_DELAY_MILLIS = 10;

  @Test
  public final void testFactorFailures() {
    final RxTxFailureModes[] presetFailureModesArray = new RxTxFailureModes[] {
                                                                               new RxTxFailureModes() {},
                                                                               new RxTxFailureModes() {{
                                                                                 rxFailureMode = new DuplicateDelivery(1);
                                                                               }},
                                                                               new RxTxFailureModes() {{
                                                                                 rxFailureMode = new DelayedDelivery(1, DELIVERY_DELAY_MILLIS);
                                                                               }},
                                                                               new RxTxFailureModes() {{
                                                                                 rxFailureMode = new DelayedDuplicateDelivery(1, DELIVERY_DELAY_MILLIS);
                                                                               }},
                                                                               new RxTxFailureModes() {{
                                                                                 txFailureMode = new DuplicateDelivery(1);
                                                                               }},
                                                                               new RxTxFailureModes() {{
                                                                                 txFailureMode = new DelayedDelivery(1, DELIVERY_DELAY_MILLIS);
                                                                               }},
                                                                               new RxTxFailureModes() {{
                                                                                 txFailureMode = new DelayedDuplicateDelivery(1, DELIVERY_DELAY_MILLIS);
                                                                               }}
    };

    for (TargetFactor target : TargetFactor.values()) {
      for (RxTxFailureModes failureModes : presetFailureModesArray) {
        zlg.i("Scenario: target: %s, failureModes: %s", z -> z.arg(target).arg(failureModes));
        boolean success = false;
        try {
          testFactorFailure(new FailureModes().set(target, failureModes));
          success = true;
        } catch (Exception e) {
          throw new AssertionError(String.format("target=%s, failureModes=%s", target, failureModes), e);
        } finally {
          if (! success) System.out.format("Failure for target=%s, failureModes=%s\n", target, failureModes);
          if (manifold != null) manifold.dispose();
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
    final AsyncInitiator initiator = new AsyncInitiator().withZlg(zlg);
    final DefaultMonitor monitor = new DefaultMonitor(new MonitorEngineConfig().withZlg(zlg));
    final Sandbox sandbox = Sandbox.forInstance(this);
    final BankBranch[] branches = BankBranch.create(2, initialBalance, true, sandbox);
    for (var branch : branches) branch.withZlg(zlg).withSendMetadata(true);

    ledger = createLedger(Guidance.COORDINATED);
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

    testSingleTransfer(initialBalance + 1, Resolution.ABORT, AbortReason.REJECT, initiator, sandbox);
    testSingleTransfer(10, Resolution.COMMIT, null, initiator, sandbox);

    wait.until(() -> {
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + List.of(branches), allZeroEscrow(branches));
      assertTrue("branches=" + List.of(branches), nonZeroBalances(branches));
    });
  }

  private void testSingleTransfer(int transferAmount, Resolution expectedVerdict, AbortReason expectedAbortReason,
                                  AsyncInitiator initiator, Sandbox sandbox) throws InterruptedException, ExecutionException, Exception {
    assert expectedVerdict == Resolution.COMMIT ^ expectedAbortReason != null;
    final String xid = UUID.randomUUID().toString();
    zlg.t("Initiating %s", z -> z.arg(xid));
    boolean success = false;
    try {
      final Outcome o = initiator.initiate(new Proposal(xid, 
                                                        TWO_BRANCH_IDS, 
                                                        BankSettlement.forTwo(transferAmount),
                                                        PROPOSAL_TIMEOUT_MILLIS).withShardKey(sandbox.key()))
          .get(FUTURE_GET_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      assertEquals(expectedVerdict, o.getResolution());
      assertEquals(expectedAbortReason, o.getAbortReason());
      success = true;
    } finally {
      if (! success) zlg.e("Error in ballot %s", z -> z.arg(xid));
    }
  }
}
