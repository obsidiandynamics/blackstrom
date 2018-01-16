package com.obsidiandynamics.blackstrom.bank;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.Wait;

public final class BankBranchTest {
  private static final String BRANCH_ID = "branch";
  private static final int INITIAL_BALANCE = 1_000;
  private static final String[] COHORTS = new String[] {BRANCH_ID};
  private static final int TTL = 1_000;
  
  private BankBranch branch;
  
  private Ledger ledger;
  
  private MessageContext context;
  
  private List<Message> received = new CopyOnWriteArrayList<>();
  
  private final Timesert wait = Wait.SHORT;
  
  @Before
  public void before() {
    branch = new BankBranch(BRANCH_ID, INITIAL_BALANCE, false);
    ledger = new SingleNodeQueueLedger();
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      received.add(m);
    });
    context = new DefaultMessageContext(ledger, null);
  }
  
  @After
  public void after() {
    branch.dispose();
    ledger.dispose();
  }
  
  @Test
  public void testNegativeXferAcceptCommit() {
    final int amount = -100;
    nominate(amount);
    wait.until(received(1));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(amount, branch.getEscrow());
    assertEquals(Pledge.ACCEPT, voteAt(0).getResponse().getPledge());
    
    // second nomination should retransmit, but have no effect on the state
    nominate(amount);
    wait.until(received(2));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(amount, branch.getEscrow());
    assertEquals(Pledge.ACCEPT, voteAt(0).getResponse().getPledge());
    
    outcome(Verdict.COMMIT);

    assertEquals(INITIAL_BALANCE + amount, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    
    // second outcome should have no effect on the state
    outcome(Verdict.COMMIT);
    
    assertEquals(INITIAL_BALANCE + amount, branch.getBalance());
    assertEquals(0, branch.getEscrow());
  }
  
  @Test
  public void testNegativeXferAcceptAbort() {
    final int amount = -100;
    nominate(amount);
    wait.until(received(1));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(amount, branch.getEscrow());
    assertEquals(Pledge.ACCEPT, voteAt(0).getResponse().getPledge());
    
    // second nomination should retransmit, but have no effect on the state
    nominate(amount);
    wait.until(received(2));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(amount, branch.getEscrow());
    assertEquals(Pledge.ACCEPT, voteAt(0).getResponse().getPledge());
    
    outcome(Verdict.ABORT);
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    
    // second outcome should have no effect on the state
    outcome(Verdict.ABORT);
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
  }
  
  @Test
  public void testNegativeXferRejectAbort() {
    final int amount = -2_000;
    nominate(amount);
    wait.until(received(1));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    assertEquals(Pledge.REJECT, voteAt(0).getResponse().getPledge());
    
    // second nomination should retransmit, but have no effect on the state
    nominate(amount);
    wait.until(received(2));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    assertEquals(Pledge.REJECT, voteAt(0).getResponse().getPledge());
    
    outcome(Verdict.ABORT);
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    
    // second outcome should have no effect on the state
    outcome(Verdict.ABORT);
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
  }
  
  
  @Test
  public void testPositiveXferAcceptCommit() {
    final int amount = 100;
    nominate(amount);
    wait.until(received(1));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    assertEquals(Pledge.ACCEPT, voteAt(0).getResponse().getPledge());
    
    // second nomination should retransmit, but have no effect on the state
    nominate(amount);
    wait.until(received(2));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    assertEquals(Pledge.ACCEPT, voteAt(0).getResponse().getPledge());
    
    outcome(Verdict.COMMIT);

    assertEquals(INITIAL_BALANCE + amount, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    
    // second outcome should have no effect on the state
    outcome(Verdict.COMMIT);
    
    assertEquals(INITIAL_BALANCE + amount, branch.getBalance());
    assertEquals(0, branch.getEscrow());
  }
  
  @Test
  public void testPositiveXferAcceptAbort() {
    final int amount = 100;
    nominate(amount);
    wait.until(received(1));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    assertEquals(Pledge.ACCEPT, voteAt(0).getResponse().getPledge());
    
    // second nomination should retransmit, but have no effect on the state
    nominate(amount);
    wait.until(received(2));
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    assertEquals(Pledge.ACCEPT, voteAt(0).getResponse().getPledge());
    
    outcome(Verdict.ABORT);
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
    
    // second outcome should have no effect on the state
    outcome(Verdict.ABORT);
    
    assertEquals(INITIAL_BALANCE, branch.getBalance());
    assertEquals(0, branch.getEscrow());
  }
  
  private Vote voteAt(int index) {
    return Cast.from(received.get(index));
  }
  
  private Runnable received(int numMessages) {
    return () -> assertEquals(numMessages, received.size());
  }
  
  private void nominate(long amount) {
    final BankSettlement settlement = new BankSettlement(Collections.singletonMap(BRANCH_ID, new BalanceTransfer(BRANCH_ID, amount)));
    branch.onNomination(context, new Nomination(0, COHORTS, settlement, TTL));
  }
  
  private void outcome(Verdict verdict) {
    final AbortReason abortReason = verdict == Verdict.COMMIT ? null : AbortReason.REJECT;
    branch.onOutcome(context, new Outcome(0, verdict, abortReason, new Response[0]));
  }
}
