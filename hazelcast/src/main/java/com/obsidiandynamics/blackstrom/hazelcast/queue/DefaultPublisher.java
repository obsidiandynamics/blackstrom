package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.worker.*;

final class DefaultPublisher implements Publisher, Joinable {
  private static final int PUBLISH_MAX_YIELDS = 100;
  private static final int PUBLISH_BACKOFF_MILLIS = 1;
  
  private static class AsyncRecord {
    final Record record;
    final PublishCallback callback;
    
    AsyncRecord(Record record, PublishCallback callback) {
      this.record = record;
      this.callback = callback;
    }
  }
  
  private final WorkerThread publishThread;
  
  private final NodeQueue<AsyncRecord> queue = new NodeQueue<>();
  
  private final QueueConsumer<AsyncRecord> queueConsumer = queue.consumer();
  
  private final Ringbuffer<byte[]> buffer;
  
  private int yields;

  DefaultPublisher(HazelcastInstance instance, PublisherConfig config) {
    buffer = StreamHelper.getRingbuffer(instance, config.getStreamConfig());
    
    publishThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withDaemon(true).withName(DefaultPublisher.class, "publisher"))
        .onCycle(this::publisherCycle)
        .buildAndStart();
  }
  
  private void publisherCycle(WorkerThread t) throws InterruptedException {
    final AsyncRecord rec = queueConsumer.poll();
    if (rec != null) {
      sendNow(rec.record, rec.callback);
      yields = 0;
    } else if (yields++ < PUBLISH_MAX_YIELDS) {
      Thread.yield();
    } else {
      Thread.sleep(PUBLISH_BACKOFF_MILLIS);
    }
  }
  
  private void sendNow(Record record, PublishCallback callback) {
    final ICompletableFuture<Long> f = buffer.addAsync(record.getData(), OverflowPolicy.OVERWRITE);
    f.andThen(new ExecutionCallback<Long>() {
      @Override public void onResponse(Long offset) {
        final long offsetPrimitive = offset;
        record.setOffset(offsetPrimitive);
        callback.onComplete(offsetPrimitive, null);
      }

      @Override public void onFailure(Throwable error) {
        callback.onComplete(Record.UNASSIGNED_OFFSET, error);
      }
    });
  }

  @Override
  public void publishAsync(Record record, PublishCallback callback) {
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
