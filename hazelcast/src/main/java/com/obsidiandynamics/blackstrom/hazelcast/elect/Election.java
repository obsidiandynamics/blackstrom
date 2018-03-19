package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

import org.slf4j.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class Election implements Terminable, Joinable {
  private static final Logger log = LoggerFactory.getLogger(Election.class);
  
  private final ElectionConfig config;
  
  private final IMap<String, byte[]> leaseTable;
  
  private final LeaseChangeHandler changeHandler;
  
  private final Registry registry;
  
  private final WorkerThread scavengerThread;
  
  private final Object scavengeLock = new Object();
  
  private volatile LeaseViewImpl leaseView = new LeaseViewImpl();
  
  public Election(ElectionConfig config, IMap<String, byte[]> leaseTable, LeaseChangeHandler changeHandler) {
    this.config = config;
    this.leaseTable = leaseTable;
    this.changeHandler = changeHandler;
    registry = new Registry(config.getInitialRegistry());
    
    scavengerThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withName(Election.class, "scavenger").withDaemon(true))
        .onCycle(this::scavegerCycle)
        .buildAndStart();
  }
  
  public Registry getRegistry() {
    return registry;
  }
  
  private void scavegerCycle(WorkerThread t) throws InterruptedException {
    scavenge();
    Thread.sleep(config.getScavengeInterval());
  }
  
  void scavenge() {
    synchronized (scavengeLock) {
      reloadView();
      
      final Set<String> resources = registry.getCandidatesView().keySet();
      for (String resource : resources) {
        final Lease existingLease = leaseView.getOrDefault(resource, Lease.vacant());
        if (! existingLease.isCurrent()) {
          if (existingLease.isVacant()) {
            log.debug("Lease of {} is vacant", resource); 
          } else {
            changeHandler.onExpire(resource, existingLease.getTenant());
            log.debug("Lease of {} by {} expired at {}", resource, existingLease.getTenant(), new Date(existingLease.getExpiry()));
          }
          
          final UUID nextCandidate = registry.getRandomCandidate(resource);
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
    }
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
  
  private void updateViewRemoveLease(String resource) {
    final LeaseViewImpl newLeaseView = new LeaseViewImpl(leaseView);
    newLeaseView.remove(resource);
    leaseView = newLeaseView;
  }

  public LeaseView getLeaseView() {
    return leaseView;
  }
  
  public void touch(String resource, UUID tenant) throws NotTenantException {
    for (;;) {
      final Lease existingLease = checkCurrent(resource, tenant);
      final Lease newLease = new Lease(tenant, System.currentTimeMillis() + config.getLeaseDuration());
      final boolean extended = leaseTable.replace(resource, existingLease.pack(), newLease.pack());
      if (extended) {
        updateViewWithLease(resource, newLease);
        return;
      } else {
        reloadView();
      }
    }
  }
  
  public void yield(String resource, UUID tenant) throws NotTenantException {
    for (;;) {
      final Lease existingLease = checkCurrent(resource, tenant);
      final boolean removed = leaseTable.remove(resource, existingLease.pack());
      if (removed) {
        updateViewRemoveLease(resource);
        return;
      } else {
        reloadView();
      }
    }
  }
  
  private Lease checkCurrent(String resource, UUID assumedTenant) throws NotTenantException {
    final Lease existingLease = leaseView.getOrDefault(resource, Lease.vacant());
    if (! existingLease.isHeldByAndCurrent(assumedTenant)) {
      final String m = String.format("Leader of %s is %s until %s", 
                                     resource, existingLease.getTenant(), new Date(existingLease.getExpiry()));
      throw new NotTenantException(m);
    } else {
      return existingLease;
    }
  }
  
  @Override
  public Joinable terminate() {
    scavengerThread.terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return scavengerThread.join(timeoutMillis);
  }
}
