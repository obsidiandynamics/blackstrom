package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.worker.*;

final class DefaultQPublisher implements QPublisher, Joinable {
  private static final int PUBLISH_MAX_YIELDS = 100;
  private static final int PUBLISH_BACKOFF_MILLIS = 1;
  
  private static class AsyncRecord {
    final RawRecord record;
    final PublishCallback callback;
    
    AsyncRecord(RawRecord record, PublishCallback callback) {
      this.record = record;
      this.callback = callback;
    }
  }
  
  private final HazelcastInstance instance;
  
  private final QPublisherConfig config;
  
  private final WorkerThread publishThread;
  
  private final NodeQueue<AsyncRecord> queue = new NodeQueue<>();
  
  private final QueueConsumer<AsyncRecord> queueConsumer = queue.consumer();
  
  private final Ringbuffer<byte[]> buffer;
  
  private int yields;

  DefaultQPublisher(HazelcastInstance instance, QPublisherConfig config) {
    this.instance = instance;
    this.config = config;
    
    final String streamFQName = QNamespace.HAZELQ_STREAM.qualify(config.getStreamConfig().getName());
    final RingbufferConfig ringbufferConfig = new RingbufferConfig(streamFQName)
        .setBackupCount(config.getStreamConfig().getSyncReplicas())
        .setAsyncBackupCount(config.getStreamConfig().getAsyncReplicas())
        .setCapacity(config.getStreamConfig().getInMemCapacity());
    instance.getConfig().addRingBufferConfig(ringbufferConfig);
    
    buffer = instance.getRingbuffer(streamFQName);
    
    publishThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withDaemon(true).withName(DefaultQPublisher.class, "publisher"))
        .onCycle(this::publishCycle)
        .buildAndStart();
  }
  
  private void publishCycle(WorkerThread t) throws InterruptedException {
    final AsyncRecord rec = queueConsumer.poll();
    if (rec != null) {
      sendNow(rec.record, rec.callback);
    } else if (yields++ < PUBLISH_MAX_YIELDS) {
      Thread.yield();
    } else {
      yields = 0;
      Thread.sleep(PUBLISH_BACKOFF_MILLIS);
    }
  }
  
  private void sendNow(RawRecord record, PublishCallback callback) {
    try {
      final long offset = buffer.add(record.getData());
      record.setOffset(offset);
      callback.onComplete(offset, null);
    } catch (Throwable e) {
      callback.onComplete(RawRecord.UNASSIGNED_OFFSET, e);
    }
  }

  @Override
  public void publishAsync(RawRecord record, PublishCallback callback) {
    queue.add(new AsyncRecord(record, callback));
  }

  @Override
  public Joinable terminate() {
    publishThread.terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return publishThread.join(timeoutMillis);
  }
}
