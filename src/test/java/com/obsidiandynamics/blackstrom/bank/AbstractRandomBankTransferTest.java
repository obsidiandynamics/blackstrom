package com.obsidiandynamics.blackstrom.bank;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runners.*;

import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractRandomBankTransferTest extends BaseBankTest {  
  @Test
  public final void testRandomTransfers() {
    final int branches = Testmark.isEnabled() ? 2 : 10;
    testRandomTransfers(branches, 100, true, true, true);
  }

  @Test
  public final void testRandomTransfersBenchmark() {
    Testmark.ifEnabled(() -> testRandomTransfers(2, 4_000_000, false, false, false));
  }

  private void testRandomTransfers(int numBranches, int runs, boolean randomiseRuns, boolean enableLogging, 
                                   boolean enableTracking) {
    final long transferAmount = 1_000;
    final long initialBalance = runs * transferAmount / (numBranches * numBranches);
    final int backlogTarget = 10_000;
    final boolean idempotencyEnabled = false;

    final AtomicInteger commits = new AtomicInteger();
    final AtomicInteger aborts = new AtomicInteger();
    final AtomicInteger timeouts = new AtomicInteger();
    final Sandbox sandbox = Sandbox.forTest(this);
    final Initiator initiator = (NullGroupInitiator) (c, o) -> {
      if (sandbox.contains(o)) {
        (o.getVerdict() == Verdict.COMMIT ? commits : o.getAbortReason() == AbortReason.REJECT ? aborts : timeouts)
        .incrementAndGet();
      }
    };
    final BankBranch[] branches = BankBranch.create(numBranches, initialBalance, idempotencyEnabled, sandbox);
    final Monitor monitor = new DefaultMonitor(new DefaultMonitorOptions().withTrackingEnabled(enableTracking));
    buildStandardManifold(initiator, monitor, branches);

    final long took = TestSupport.took(() -> {
      String[] branchIds = null;
      BankSettlement settlement = null;
      if (! randomiseRuns) {
        branchIds = numBranches != 2 ? BankBranch.generateIds(numBranches) : TWO_BRANCH_IDS;
        settlement = BankSettlement.randomise(branchIds, transferAmount);
      }
      
      final long ballotIdBase = System.currentTimeMillis() << 32;
      for (int run = 0; run < runs; run++) {
        if (randomiseRuns) {
          branchIds = numBranches != 2 ? BankBranch.generateIds(2 + (int) (Math.random() * (numBranches - 1))) : TWO_BRANCH_IDS;
          settlement = BankSettlement.randomise(branchIds, transferAmount);
        }
        final Proposal p = new Proposal(Long.toHexString(ballotIdBase + run), branchIds, settlement, PROPOSAL_TIMEOUT)
            .withShardKey(sandbox.key());
        if (TestSupport.LOG) TestSupport.LOG_STREAM.format("proposing %s\n", p);
        ledger.append(p);

        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = (int) (run - commits.get() - aborts.get());
            if (backlog > backlogTarget) {
              TestSupport.sleep(1);
              if (enableLogging && System.currentTimeMillis() - lastLogTime > 5_000) {
                TestSupport.LOG_STREAM.format("throttling... backlog @ %,d (%,d txns)\n", backlog, run);
                lastLogTime = System.currentTimeMillis();
              }
            } else {
              break;
            }
          }
        }
      }

      wait.until(() -> {
        assertEquals(runs, commits.get() + aborts.get() + timeouts.get());
        final long expectedBalance = numBranches * initialBalance;
        assertEquals(expectedBalance, getTotalBalance(branches));
        assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
        assertTrue("branches=" + Arrays.asList(branches), nonZeroBalances(branches));
      });
    });
    System.out.format("%,d took %,d ms, %,.0f txns/sec (%,d commits | %,d aborts | %,d timeouts)\n", 
                      runs, took, (double) runs / took * 1000, commits.get(), aborts.get(), timeouts.get());
  }
}
