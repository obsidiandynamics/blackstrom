package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;
import java.util.concurrent.*;

import org.slf4j.*;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class DefaultSubscriber implements Subscriber, Joinable {
  private static final int PUBLISH_MAX_YIELDS = 100;
  private static final int PUBLISH_BACKOFF_MILLIS = 1;
  
  private static final Logger log = LoggerFactory.getLogger(DefaultSubscriber.class);
  
  private final SubscriberConfig config;
  
  private final Ringbuffer<byte[]> buffer;
  
  private final IMap<String, Long> groupOffsets;
  
  private final Election election;
  
  private final UUID leaseCandidate;
  
  private final WorkerThread confirmThread;
  
  private long nextOffset;
  
  private volatile long lastScheduledOffset = Record.UNASSIGNED_OFFSET;
  
  private long lastConfirmedOffset = lastScheduledOffset;
  
  private int yields;
  
  DefaultSubscriber(HazelcastInstance instance, SubscriberConfig config) {
    this.config = config;
    
    final StreamConfig streamConfig = config.getStreamConfig();
    buffer = StreamHelper.getRingbuffer(instance, streamConfig);
    
    if (config.hasGroup()) {
      final String offsetsFQName = QNamespace.HAZELQ_META.qualify("offsets." + streamConfig.getName());
      groupOffsets = instance.getMap(offsetsFQName);
      nextOffset = Record.UNASSIGNED_OFFSET;
      
      final String leaseFQName = QNamespace.HAZELQ_META.qualify("lease." + streamConfig.getName());
      final IMap<String, byte[]> leaseTable = instance.getMap(leaseFQName);
      leaseCandidate = UUID.randomUUID();
      election = new Election(config.getElectionConfig(), leaseTable, new LeaseChangeHandler() {
        @Override public void onExpire(String resource, UUID tenant) {
          log.debug("Expired lease of {} held by {}", resource, tenant);
        }
        
        @Override public void onAssign(String resource, UUID tenant) {
          log.debug("Assigned lease of {} to {}", resource, tenant);
        }
      });
      election.getRegistry().enroll(config.getGroup(), leaseCandidate);
      
      confirmThread = WorkerThread.builder()
          .withOptions(new WorkerOptions().withDaemon(true).withName(DefaultSubscriber.class, "confirm"))
          .onCycle(this::confirmCycle)
          .buildAndStart();
    } else {
      groupOffsets = null;
      election = null;
      leaseCandidate = null;
      confirmThread = null;
    }
  }
  
  private void confirmCycle(WorkerThread t) throws InterruptedException {
    final long lastScheduledOffset = this.lastScheduledOffset;
    
    if (lastScheduledOffset != lastConfirmedOffset) {
      confirmOffset(lastScheduledOffset);
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
      lastConfirmedOffset = offset;
    } else {
      log.warn("Failed confirming offset {} for stream {}: {} is not the current tenant for group {}",
               offset, config.getStreamConfig().getName(), leaseCandidate, config.getGroup());
    }
  }
  
  private boolean isCurrentTenant() {
    return election.getLeaseView().isCurrentTenant(config.getGroup(), leaseCandidate);
  }

  @Override
  public RecordBatch poll(long timeoutMillis) throws InterruptedException {
    if (leaseCandidate == null || isCurrentTenant()) {
      if (nextOffset == Record.UNASSIGNED_OFFSET) {
        nextOffset = loadConfirmedOffset() + 1;
      }
      
      final ICompletableFuture<ReadResultSet<byte[]>> f = buffer.readManyAsync(nextOffset, 1, 1000, null);
      try {
        final ReadResultSet<byte[]> resultSet = f.get(timeoutMillis, TimeUnit.MILLISECONDS);
        nextOffset += resultSet.size();
        return readBatch(resultSet);
      } catch (ExecutionException e) {
        log.warn(String.format("Error reading at offset %d from stream %s",
                               nextOffset, config.getStreamConfig().getName()), e.getCause());
        f.cancel(true);
        return RecordBatch.empty();
      } catch (TimeoutException e) {
        f.cancel(true);
        return RecordBatch.empty();
      }
    } else {
      nextOffset = Record.UNASSIGNED_OFFSET;
      Thread.sleep(timeoutMillis);
      return RecordBatch.empty();
    }
  }
  
  private long loadConfirmedOffset() {
    return groupOffsets.get(config.getGroup());
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
  public void confirm(long offset) {
    if (leaseCandidate != null) {
      lastScheduledOffset = offset;
    }
  }

  @Override
  public Joinable terminate() {
    if (confirmThread != null) confirmThread.terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return confirmThread != null ? confirmThread.join(timeoutMillis) : true;
  }
}
