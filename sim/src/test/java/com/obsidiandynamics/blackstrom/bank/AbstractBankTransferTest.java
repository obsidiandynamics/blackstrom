package com.obsidiandynamics.blackstrom.bank;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;

public abstract class AbstractBankTransferTest extends BaseBankTest {  
  @Test
  public final void testCommit() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final Sandbox sandbox = Sandbox.forTest(this);
    final BankBranch[] branches = BankBranch.create(2, initialBalance, true, sandbox);
    buildStandardManifold(initiator, monitor, branches);

    final Outcome o = initiator.initiate(new Proposal(UUID.randomUUID().toString(), 
                                                      TWO_BRANCH_IDS,
                                                      BankSettlement.forTwo(transferAmount),
                                                      PROPOSAL_TIMEOUT).withShardKey(sandbox.key()))
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.COMMIT, o.getVerdict());
    assertNull(o.getAbortReason());
    assertEquals(2, o.getResponses().length);
    assertEquals(Intent.ACCEPT, o.getResponse(BankBranch.getId(0)).getIntent());
    assertEquals(Intent.ACCEPT, o.getResponse(BankBranch.getId(1)).getIntent());
    wait.until(() -> {
      assertEquals(initialBalance - transferAmount, branches[0].getBalance());
      assertEquals(initialBalance + transferAmount, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
    });
  }

  @Test
  public final void testAbort() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance + 1;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final Sandbox sandbox = Sandbox.forTest(this);
    final BankBranch[] branches = BankBranch.create(2, initialBalance, true, sandbox);
    buildStandardManifold(initiator, monitor, branches);

    final Outcome o = initiator.initiate(new Proposal(UUID.randomUUID().toString(), 
                                                      TWO_BRANCH_IDS, 
                                                      BankSettlement.forTwo(transferAmount),
                                                      PROPOSAL_TIMEOUT).withShardKey(sandbox.key()))
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertEquals(AbortReason.REJECT, o.getAbortReason());
    assertTrue("responses.length=" + o.getResponses().length, o.getResponses().length >= 1); // the accept status doesn't need to have been considered
    assertEquals(Intent.REJECT, o.getResponse(BankBranch.getId(0)).getIntent());
    final Response acceptResponse = o.getResponse(BankBranch.getId(1));
    if (acceptResponse != null) {
      assertEquals(Intent.ACCEPT, acceptResponse.getIntent());  
    }
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
    });
  }

  @Test
  public final void testImplicitTimeout() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(60_000));
    final Sandbox sandbox = Sandbox.forTest(this);
    final BankBranch[] branches = BankBranch.create(2, initialBalance, true, sandbox);
    // we delay the receive rather than the send, so that the send timestamp appears recent — triggering implicit timeout
    buildStandardManifold(initiator, 
                          monitor, 
                          branches[0], 
                          new FallibleFactor(branches[1]).withRxFailureMode(new DelayedDelivery(1, 10)));

    final Outcome o = initiator.initiate(new Proposal(UUID.randomUUID().toString(),
                                                      TWO_BRANCH_IDS, 
                                                      BankSettlement.forTwo(transferAmount),
                                                      1).withShardKey(sandbox.key()))
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertEquals(AbortReason.IMPLICIT_TIMEOUT, o.getAbortReason());
    assertTrue("responses.length=" + o.getResponses().length, o.getResponses().length >= 1);
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
    });
  }

  @Test
  public final void testExplicitTimeout() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(1));
    final Sandbox sandbox = Sandbox.forTest(this);
    final BankBranch[] branches = BankBranch.create(2, initialBalance, true, sandbox);
    // it doesn't matter whether we delay receive or send, since the messages are sufficiently delayed, such
    // that they won't get there within the test's running time — either failure mode will trigger an explicit timeout
    buildStandardManifold(initiator, 
                          monitor, 
                          new FallibleFactor(branches[0]).withRxFailureMode(new DelayedDelivery(1, 60_000)),
                          new FallibleFactor(branches[1]).withRxFailureMode(new DelayedDelivery(1, 60_000)));

    final Outcome o = initiator.initiate(new Proposal(UUID.randomUUID().toString(),
                                                      TWO_BRANCH_IDS, 
                                                      BankSettlement.forTwo(transferAmount),
                                                      1).withShardKey(sandbox.key()))
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertEquals(AbortReason.EXPLICIT_TIMEOUT, o.getAbortReason());
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
    });
  }
}
