package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

import org.slf4j.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class Election implements Joinable {
  private static final Logger log = LoggerFactory.getLogger(Election.class);
  
  private final ElectionConfig config;
  
  private final IMap<String, byte[]> leaseTable;
  
  private final LeaseChangeHandler changeHandler;
  
  private final Register register;
  
  private final WorkerThread scavengerThread;
  
  private volatile LeaseViewImpl leaseView = new LeaseViewImpl();
  
  public Election(ElectionConfig config, IMap<String, byte[]> leaseTable, LeaseChangeHandler changeHandler) {
    this.config = config;
    this.leaseTable = leaseTable;
    this.changeHandler = changeHandler;
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
        changeHandler.onExpire(resource, existingLease.getTenant());
        if (existingLease.isVacant()) {
          log.debug("Lease of {} is vacant", resource); 
        } else {
          log.debug("Lease of {} by {} expired at {}", resource, existingLease.getTenant(), new Date(existingLease.getExpiry()));
        }
        
        final UUID nextCandidate = register.getRandomCandidate(resource);
        if (nextCandidate != null) {
          final boolean success;
          final Lease newLease = new Lease(nextCandidate, System.currentTimeMillis() + config.getLeaseDuration());
          if (existingLease.isVacant()) {
            final byte[] previous = leaseTable.putIfAbsent(resource, newLease.pack());
            success = previous == null;
          } else {
            success = leaseTable.replace(resource, existingLease.pack(), newLease.pack());
          }
          
          if (success) {
            log.debug("New lease of {} by {} until {}", resource, nextCandidate, new Date(newLease.getExpiry()));
            updateViewWithLease(resource, newLease);
            changeHandler.onAssign(resource, nextCandidate);
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
  
  public void touch(String resource, UUID tenant) throws NotLeaderException {
    for (;;) {
      final Lease existingLease = leaseView.getOrDefault(resource, Lease.VACANT);
      if (existingLease.isHeldByAndCurrent(tenant)) {
        final Lease newLease = new Lease(tenant, System.currentTimeMillis() + config.getLeaseDuration());
        final boolean updated = leaseTable.replace(resource, existingLease.pack(), newLease.pack());
        if (updated) {
          updateViewWithLease(resource, newLease);
          return;
        } else {
          reloadView();
        }
      } else {
        final String m = String.format("Leader of %s is %s until %s", resource, existingLease.getTenant(), new Date(existingLease.getExpiry()));
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
