package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

import org.slf4j.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class Election implements Joinable {
  private static final Logger log = LoggerFactory.getLogger(Election.class);
  
  private final ElectionConfig config;
  
  private final IMap<String, byte[]> leaseTable;
  
  private final LeadershipAssignmentHandler assignmentHandler;
  
  private final Register register;
  
  private final WorkerThread scavengerThread;
  
  private volatile LeaseViewImpl leaseView = new LeaseViewImpl();
  
  public Election(ElectionConfig config, IMap<String, byte[]> leaseTable, LeadershipAssignmentHandler assignmentHandler) {
    this.config = config;
    this.leaseTable = leaseTable;
    this.assignmentHandler = assignmentHandler;
    register = new Register();
    
    scavengerThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withName(Election.class, "scavenger").withDaemon(true))
        .onCycle(this::scavegeCycle)
        .buildAndStart();
  }
  
  public Register getRegister() {
    return register;
  }
  
  private void scavegeCycle(WorkerThread t) throws InterruptedException {
    reloadView();
    
    final Set<String> resources = register.getCandidatesView().keySet();
    for (String resource : resources) {
      final Lease existingLease = leaseView.getOrDefault(resource, Lease.VACANT);
      if (! existingLease.isCurrent()) {
        if (existingLease.isVacant()) {
          log.debug("Lease of {} is vacant", resource); 
        } else {
          log.debug("Lease of {} held by {} expired at {}", resource, existingLease.getCandidateId(), new Date(existingLease.getExpiry()));
        }
        
        final UUID nextCandidateId = register.getRandomCandidate(resource);
        if (nextCandidateId != null) {
          final boolean success;
          final Lease newLease = new Lease(nextCandidateId, System.currentTimeMillis() + config.getLeaseDuration());
          if (existingLease.isVacant()) {
            final byte[] previous = leaseTable.putIfAbsent(resource, newLease.pack());
            success = previous == null;
          } else {
            success = leaseTable.replace(resource, existingLease.pack(), newLease.pack());
          }
          
          if (success) {
            log.debug("New lease of {} by {} until {}", resource, nextCandidateId, new Date(newLease.getExpiry()));
            updateViewWithLease(resource, newLease);
            assignmentHandler.onAssign(resource, nextCandidateId);
          }
        }
      }
    }
    
    Thread.sleep(config.getScavengeInterval());
  }
  
  private void reloadView() {
    final LeaseViewImpl newLeaseView = new LeaseViewImpl();
    for (Map.Entry<String, byte[]> leaseTableEntry : leaseTable.entrySet()) {
      final Lease lease = Lease.unpack(leaseTableEntry.getValue());
      newLeaseView.put(leaseTableEntry.getKey(), lease);
    }
    leaseView = newLeaseView;
  }
  
  private void updateViewWithLease(String resource, Lease lease) {
    final LeaseViewImpl newLeaseView = new LeaseViewImpl(leaseView);
    newLeaseView.put(resource, lease);
    leaseView = newLeaseView;
  }

  public LeaseView getLeaseView() {
    return leaseView;
  }
  
  public void touch(String resource, UUID candidateId) throws NotLeaderException {
    for (;;) {
      final Lease existingLease = leaseView.getOrDefault(resource, Lease.VACANT);
      if (existingLease.isHeldByAndCurrent(candidateId)) {
        final Lease newLease = new Lease(candidateId, System.currentTimeMillis() + config.getLeaseDuration());
        final boolean updated = leaseTable.replace(resource, existingLease.pack(), newLease.pack());
        if (updated) {
          updateViewWithLease(resource, newLease);
          return;
        } else {
          reloadView();
        }
      } else {
        final String m = String.format("Leader of %s is %s until %s", resource, existingLease.getCandidateId(), new Date(existingLease.getExpiry()));
        throw new NotLeaderException(m, null);
      }
    }
  }
  
  public Joinable terminate() {
    scavengerThread.terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return scavengerThread.join(timeoutMillis);
  }
}
