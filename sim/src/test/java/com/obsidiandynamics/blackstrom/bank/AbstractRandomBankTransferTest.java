package com.obsidiandynamics.blackstrom.bank;

import static org.junit.Assert.*;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runners.*;

import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.testmark.Scale;
import com.obsidiandynamics.testmark.Testmark;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.zerolog.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractRandomBankTransferTest extends BaseBankTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private final int SCALE = Testmark.getOptions(Scale.class, Scale.unity()).magnitude();
  
  private static final boolean LOG_BENCHMARK = false;
  
  @Test
  public final void testRandomTransfersAutonomous() {
    final int branches = Testmark.isEnabled() ? 2 : 10;
    testRandomTransfers(branches, 100 * SCALE, true, true, true, true);
  }
  
  @Test
  public final void testRandomTransfersCoordinated() {
    final int branches = Testmark.isEnabled() ? 2 : 10;
    testRandomTransfers(branches, 100 * SCALE, true, true, true, false);
  }

  @Test
  public final void testRandomTransfersAutonomousBenchmark() {
    Testmark.ifEnabled("autonomous", () -> testRandomTransfers(2, 4_000_000 * SCALE, false, LOG_BENCHMARK, false, true));
  }

  @Test
  public final void testRandomTransfersCoordinatedBenchmark() {
    Testmark.ifEnabled("coordinated", () -> testRandomTransfers(2, 4_000_000 * SCALE, false, LOG_BENCHMARK, false, false));
  }

  private void testRandomTransfers(int numBranches, int runs, boolean randomiseRuns, boolean loggingEnabled, 
                                   boolean trackingEnabled, boolean autonomous) {
    final long transferAmount = 1_000;
    final long initialBalance = runs * transferAmount / (numBranches * numBranches);
    final int backlogTarget = Math.min(runs / 10, 10_000);
    final boolean idempotencyEnabled = false;

    final AtomicInteger commits = new AtomicInteger();
    final AtomicInteger aborts = new AtomicInteger();
    final AtomicInteger timeouts = new AtomicInteger();
    
    final long started = System.currentTimeMillis();
    final WorkerThread progressMonitorThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(AbstractBankTransferTest.class, "progress"))
        .onCycle(__thread -> {
          Thread.sleep(2000);
          final int c = commits.get(), a = aborts.get(), t = timeouts.get(), s = c + a + t;
          final long took = System.currentTimeMillis() - started;
          final double rate = 1000d * s / took;
          System.out.format("%,d commits | %,d aborts | %,d timeouts | %,d total [%,.0f/s]\n", 
                            c, a, t, s, rate);
        })
        .buildAndStart();
    
    final Sandbox sandbox = Sandbox.forInstance(this);
    final Initiator initiator = (NullGroupInitiator) (c, o) -> {
      if (sandbox.contains(o)) {
        (o.getResolution() == Resolution.COMMIT ? commits : o.getAbortReason() == AbortReason.REJECT ? aborts : timeouts)
        .incrementAndGet();
      }
    };
    final BankBranch[] branches = BankBranch.create(numBranches, initialBalance, idempotencyEnabled, sandbox);
    if (autonomous) {
      buildAutonomousManifold(new MonitorEngineConfig().withTrackingEnabled(trackingEnabled),
                              initiator, 
                              branches);
    } else {
      buildCoordinatedManifold(new MonitorEngineConfig().withTrackingEnabled(trackingEnabled),
                               initiator, 
                               branches);
    }

    final long tookMillis = Threads.tookMillis(() -> {
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
        zlg.t("proposing %s", z -> z.arg(p));
        ledger.append(p);

        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = (int) (run - getMinOutcomes(branches));
            if (backlog >= backlogTarget) {
              Threads.sleep(1);
              if (loggingEnabled && System.currentTimeMillis() - lastLogTime > 5_000) {
                final int _run = run;
                zlg.i("throttling... backlog @ %,d (%,d txns)", z -> z.arg(backlog).arg(_run));
                lastLogTime = System.currentTimeMillis();
              }
            } else {
              break;
            }
          }
        }
      }
      progressMonitorThread.terminate().joinSilently();
      
      wait.until(() -> {
        assertEquals(runs, commits.get() + aborts.get() + timeouts.get());
        final long expectedBalance = numBranches * initialBalance;
        assertEquals(expectedBalance, getTotalBalance(branches));
        assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
        assertTrue("branches=" + Arrays.asList(branches), nonZeroBalances(branches));
      });
    });
    System.out.format("%,d took %,d ms, %,.0f txns/sec (%,d commits | %,d aborts | %,d timeouts)\n", 
                      runs, tookMillis, (double) runs / tookMillis * 1000, commits.get(), aborts.get(), timeouts.get());
  }
  
  private long getMinOutcomes(BankBranch[] branches) {
    long minOutcomes = Long.MAX_VALUE;
    for (BankBranch branch : branches) {
      final long outcomes = branch.getNumOutcomes();
      if (outcomes < minOutcomes) {
        minOutcomes = outcomes;
      }
    }
    return minOutcomes;
  }
}
