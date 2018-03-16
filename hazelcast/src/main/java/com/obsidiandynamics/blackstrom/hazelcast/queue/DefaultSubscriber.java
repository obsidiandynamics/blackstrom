package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;
import java.util.concurrent.*;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class DefaultSubscriber implements Subscriber, Joinable {
  private static final int PUBLISH_MAX_YIELDS = 100;
  private static final int PUBLISH_BACKOFF_MILLIS = 1;
  
  private final SubscriberConfig config;
  
  private final Ringbuffer<byte[]> buffer;
  
  private final IMap<String, Long> groupOffsets;
  
  private final Election election;
  
  private final UUID leaseCandidate;
  
  private final WorkerThread groupThread;
  
  private long nextReadOffset;
  
  private long lastReadOffset = Record.UNASSIGNED_OFFSET;
  
  private volatile long scheduledConfirmOffset = Record.UNASSIGNED_OFFSET;
  
  private long lastConfirmedOffset = scheduledConfirmOffset;
  
  private volatile long scheduledTouchTimestamp = 0;
  
  private long lastTouchedTimestamp = scheduledTouchTimestamp;
  
  private int yields;
  
  DefaultSubscriber(HazelcastInstance instance, SubscriberConfig config) {
    this.config = config;
    
    final StreamConfig streamConfig = config.getStreamConfig();
    buffer = StreamHelper.getRingbuffer(instance, streamConfig);
    
    if (config.hasGroup()) {
      final String offsetsFQName = QNamespace.HAZELQ_META.qualify("offsets." + streamConfig.getName());
      groupOffsets = instance.getMap(offsetsFQName);
      nextReadOffset = Record.UNASSIGNED_OFFSET;
      
      final String leaseFQName = QNamespace.HAZELQ_META.qualify("lease." + streamConfig.getName());
      final IMap<String, byte[]> leaseTable = instance.getMap(leaseFQName);
      leaseCandidate = UUID.randomUUID();
      election = new Election(config.getElectionConfig(), leaseTable, new LeaseChangeHandler() {
        @Override public void onExpire(String resource, UUID tenant) {
          config.getLog().debug("Expired lease of {} held by {}", resource, tenant);
        }
        
        @Override public void onAssign(String resource, UUID tenant) {
          config.getLog().debug("Assigned lease of {} to {}", resource, tenant);
        }
      });
      election.getRegistry().enroll(config.getGroup(), leaseCandidate);
      
      groupThread = WorkerThread.builder()
          .withOptions(new WorkerOptions().withDaemon(true).withName(DefaultSubscriber.class, "group"))
          .onCycle(this::groupCycle)
          .buildAndStart();
    } else {
      groupOffsets = null;
      election = null;
      leaseCandidate = null;
      groupThread = null;
    }
  }
  
  Election getElection() {
    return election;
  }

  @Override
  public RecordBatch poll(long timeoutMillis) throws InterruptedException {
    final boolean isGroupSubscriber = leaseCandidate != null;
    final boolean isCurrentTenant = isGroupSubscriber && isCurrentTenant();
    
    if (! isGroupSubscriber || isCurrentTenant) {
      if (nextReadOffset == Record.UNASSIGNED_OFFSET) {
        nextReadOffset = loadConfirmedOffset() + 1;
      }
      
      final ICompletableFuture<ReadResultSet<byte[]>> f = buffer.readManyAsync(nextReadOffset, 1, 1_000, StreamHelper.NOT_NULL);
      try {
        final ReadResultSet<byte[]> resultSet = f.get(timeoutMillis, TimeUnit.MILLISECONDS);
        lastReadOffset = resultSet.getSequence(resultSet.size() - 1);
        nextReadOffset = lastReadOffset + 1;
        return readBatch(resultSet);
      } catch (ExecutionException e) {
        final String m = String.format("Error reading at offset %d from stream %s",
                                       nextReadOffset, config.getStreamConfig().getName());
        config.getErrorHandler().onError(m, e);
        f.cancel(true);
        return RecordBatch.empty();
      } catch (TimeoutException e) {
        f.cancel(true);
        return RecordBatch.empty();
      } finally {
        if (isCurrentTenant) {
          scheduledTouchTimestamp = System.currentTimeMillis();
        }
      }
    } else {
      nextReadOffset = Record.UNASSIGNED_OFFSET;
      Thread.sleep(timeoutMillis);
      return RecordBatch.empty();
    }
  }
  
  private long loadConfirmedOffset() {
    final Long confirmedOffset = groupOffsets.get(config.getGroup());
    if (confirmedOffset != null) {
      return confirmedOffset;
    } else {
      // TODO offset reset
      return -1;
    }
  }
  
  private static RecordBatch readBatch(ReadResultSet<byte[]> resultSet) {
    final List<Record> records = new ArrayList<>(resultSet.size());
    long offset = resultSet.getSequence(0);
    for (byte[] result : resultSet) {
      records.add(new Record(result, offset++));
    }
    return new RecordBatch(records);
  }
  
  @Override
  public void confirm() {
    if (lastReadOffset != Record.UNASSIGNED_OFFSET) {
      confirm(lastReadOffset);
    }
  }

  @Override
  public void confirm(long offset) {
    if (offset < StreamHelper.SMALLEST_OFFSET || offset > lastReadOffset) {
      throw new IllegalArgumentException(String.format("Illegal offset %d; last read %d", offset, lastReadOffset));
    }
    
    if (leaseCandidate != null) {
      scheduledConfirmOffset = offset;
    }
  }
  
  @Override
  public void seek(long offset) {
    if (offset < StreamHelper.SMALLEST_OFFSET) throw new IllegalArgumentException("Invalid seek offset " + offset);
    nextReadOffset = offset;
  }
  
  private void groupCycle(WorkerThread t) throws InterruptedException {
    final long scheduledConfirmOffset = this.scheduledConfirmOffset;
    final long scheduledTouchTimestamp = this.scheduledTouchTimestamp;
    
    boolean performedWork = false;
    if (scheduledConfirmOffset != lastConfirmedOffset) {
      performedWork = true;
      confirmOffset(scheduledConfirmOffset);
    }
    
    if (scheduledTouchTimestamp != lastTouchedTimestamp) {
      performedWork = true;
      touchLease(scheduledTouchTimestamp);
    }
    
    if (performedWork) {
      yields = 0;
    } else if (yields++ < PUBLISH_MAX_YIELDS) {
      Thread.yield();
    } else {
      Thread.sleep(PUBLISH_BACKOFF_MILLIS);
    }
  }
  
  private void confirmOffset(long offset) {
    if (isCurrentTenant()) {
      groupOffsets.put(config.getGroup(), offset);
    } else {
      final String m = String.format("Failed confirming offset %s for stream %s: %s is not the current tenant for group %s",
                                     offset, config.getStreamConfig().getName(), leaseCandidate, config.getGroup());
      config.getErrorHandler().onError(m, null);
    }
    lastConfirmedOffset = offset;
  }
  
  private void touchLease(long timestamp) {
    try {
      election.touch(config.getGroup(), leaseCandidate);
    } catch (NotTenantException e) {
      config.getErrorHandler().onError("Failed to extend lease", e);
    }
    lastTouchedTimestamp = timestamp;
  }
  
  private boolean isCurrentTenant() {
    return election.getLeaseView().isCurrentTenant(config.getGroup(), leaseCandidate);
  }
  
  @Override
  public boolean isAssigned() {
    return isCurrentTenant();
  }

  @Override
  public Joinable terminate() {
    if (groupThread != null) groupThread.terminate();
    if (election != null) election.terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return groupThread != null ? Joinable.joinAll(timeoutMillis, groupThread, election) : true;
  }
}
