package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;
import java.util.concurrent.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class DefaultMonitor implements Monitor {
  static final boolean DEBUG = false;
  
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMonitor.class);
  
  private Ledger ledger;
  
  private final Map<Object, PendingBallot> pending = new HashMap<>();
  
  private final Map<Object, Outcome> decided = new ConcurrentHashMap<>();
  private final BlockingQueue<Outcome> additions = new LinkedBlockingQueue<>();
  
  private final String groupId;
  
  private final WorkerThread gcThread;
  
  private final int gcIntervalMillis;
  
  private final int outcomeLifetimeMillis;
  
  private long reapedSoFar;
  
  private final WorkerThread timeoutThread;
  
  private final int timeoutIntervalMillis;
  
  private final Object lock = new Object();
  
  private final ShardedFlow flow = new ShardedFlow();
  
  public DefaultMonitor() {
    this(new DefaultMonitorOptions());
  }
  
  public DefaultMonitor(DefaultMonitorOptions options) {
    this.groupId = options.getGroupId();
    this.gcIntervalMillis = options.getGCInterval();
    this.outcomeLifetimeMillis = options.getOutcomeLifetime();
    this.timeoutIntervalMillis = options.getTimeoutInterval();
    
    gcThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(nameThread("gc"))
                     .withDaemon(true))
        .onCycle(this::gcCycle)
        .buildAndStart();
    
    timeoutThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(nameThread("timeout"))
                     .withDaemon(true))
        .onCycle(this::timeoutCycle)
        .buildAndStart();
  }
  
  private String nameThread(String role) {
    return DefaultMonitor.class.getSimpleName() + "-" + role + "-" + Integer.toHexString(System.identityHashCode(this));
  }

  @Override
  public String getGroupId() {
    return groupId;
  }
  
  private void gcCycle(WorkerThread thread) throws InterruptedException {
    Thread.sleep(gcIntervalMillis);
    
    final long collectThreshold = System.currentTimeMillis() - outcomeLifetimeMillis;
    int reaped = 0;
    for (Outcome outcome : decided.values()) {
      if (outcome.getTimestamp() < collectThreshold) {
        decided.remove(outcome.getBallotId());
        reaped++;
      }
    }
    
    for (;;) {
      final Outcome addition = additions.poll();
      if (addition != null) {
        decided.put(addition.getBallotId(), addition);
      } else {
        break;
      }
    }
      
    if (reaped != 0) {
      reapedSoFar += reaped;
      LOG.debug("Reaped {} outcomes ({} so far), pending: {}, decided: {}", 
                reaped, reapedSoFar, pending.size(), decided.size());
    }
  }
  
  private void timeoutCycle(WorkerThread thread) throws InterruptedException {
    Thread.sleep(timeoutIntervalMillis);
    
    final List<PendingBallot> pendingCopy;
    synchronized (lock) {
      pendingCopy = new ArrayList<>(pending.values());
    }
    
    for (PendingBallot pending : pendingCopy) {
      final Proposal proposal = pending.getProposal();
      if (proposal.getTimestamp() + proposal.getTtl() < System.currentTimeMillis()) {
        for (String cohort : proposal.getCohorts()) {
          final boolean cohortResponded;
          synchronized (lock) {
            cohortResponded = pending.hasResponded(cohort);
          }
          
          if (! cohortResponded) {
            timeoutCohort(proposal, cohort);
          }
        }
      }
    }
  }
  
  private void timeoutCohort(Proposal proposal, String cohort) {
    LOG.debug("Timed out {} for cohort {}", proposal, cohort);
    append(new Vote(proposal.getBallotId(), new Response(cohort, Intent.TIMEOUT, null)).inResponseTo(proposal));
  }
  
  private void append(Message message) {
    ledger.append(message, (id, x) -> {
      if (x != null) LOG.warn("Error appending to ledger [message: " + message + "]", x);
    });
  }
  
  public Map<Object, Outcome> getOutcomes() {
    final Map<Object, Outcome> decidedCopy;
    synchronized (lock) {
      decidedCopy = new HashMap<>(decided);
    }
    return Collections.unmodifiableMap(decidedCopy);
  }
  
  @Override
  public void onProposal(MessageContext context, Proposal proposal) {
    if (decided.containsKey(proposal.getBallotId())) {
      if (DEBUG) LOG.trace("Skipping redundant {} (ballot already decided)", proposal);
      return;
    }
    
    synchronized (lock) {
      final PendingBallot newBallot = new PendingBallot(proposal);
      final PendingBallot existingBallot = pending.put(proposal.getBallotId(), newBallot);
      if (existingBallot != null) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already pending)", proposal);
        pending.put(proposal.getBallotId(), existingBallot);
        return;
      } else {
        newBallot.setConfirmation(flow.begin(context, proposal));
      }
    }
    
    if (DEBUG) LOG.trace("Initiating ballot for {}", proposal);
  }

  @Override
  public void onVote(MessageContext context, Vote vote) {
    synchronized (lock) {
      final PendingBallot ballot = pending.get(vote.getBallotId());
      if (ballot != null) {
        if (DEBUG) LOG.trace("Received {}", vote);
        final boolean decided = ballot.castVote(LOG, vote);
        if (decided) {
          decideBallot(ballot);
        }
      } else if (decided.containsKey(vote.getBallotId())) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already decided)", vote);
      } else {
        if (DEBUG) LOG.trace("Missing pending ballot for vote {}", vote);
      }
    }
  }
  
  private void decideBallot(PendingBallot ballot) {
    if (DEBUG) LOG.trace("Decided ballot for {}: verdict: {}", ballot.getProposal(), ballot.getVerdict());
    final Proposal proposal = ballot.getProposal();
    final String ballotId = proposal.getBallotId();
    final Outcome outcome = new Outcome(ballotId, ballot.getVerdict(), ballot.getAbortReason(), ballot.getResponses())
        .inResponseTo(proposal);
    pending.remove(ballotId);
    enqueue(outcome);
    ledger.append(outcome, (id, x) -> {
      if (x == null) {
        ballot.getConfirmation().confirm();
      } else {
        LOG.warn("Error appending to ledger [message: " + outcome + "]", x);
      }
    });
  }
  
  private void enqueue(Outcome outcome) {
    additions.offer(outcome);
  }
  
  @Override
  public void init(InitContext context) {
    ledger = context.getLedger();
  }
  
  @Override
  public void dispose() {
    gcThread.terminate();
    timeoutThread.terminate();
    gcThread.joinQuietly();
    timeoutThread.joinQuietly();
    flow.dispose();
  }
}
