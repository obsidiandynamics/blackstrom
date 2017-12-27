package obsidiandynamics.blackstrom.ledger.multiqueue;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.yconf.*;

import obsidiandynamics.blackstrom.handler.*;
import obsidiandynamics.blackstrom.ledger.*;
import obsidiandynamics.blackstrom.model.*;
import obsidiandynamics.blackstrom.worker.*;

/**
 *  Simple, in-memory ledger implementation comprising multiple discrete blocking queues - one for each
 *  subscriber.
 */
@Y
public final class MultiQueueLedger implements Ledger {
  private final List<BlockingQueue<Message>> queues = new ArrayList<>();
  private final List<WorkerThread> threads = new CopyOnWriteArrayList<>();
  private final Object lock = new Object();
  
  private final int capacity;
  
  public MultiQueueLedger(@YInject(name="capacity") int capacity) {
    this.capacity = capacity;
  }

  @Override
  public void attach(MessageHandler handler) {
    final BlockingQueue<Message> queue = new LinkedBlockingQueue<>(capacity);
    final WorkerThread thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName("MessageWorker-" + Long.toHexString(System.identityHashCode(handler)))
                     .withDaemon(true))
        .withWorker(t -> consumeAndDispatch(queue, t, handler))
        .build();
    threads.add(thread);
    thread.start();
    synchronized (lock) {
      queues.add(queue);
    }
  }
  
  private void consumeAndDispatch(BlockingQueue<Message> queue, WorkerThread thread, MessageHandler handler) throws InterruptedException {
    final Message m = queue.take();
    handler.onMessage(new VotingContext() {}, m);
  }
  
  @Override
  public void append(Message message) throws InterruptedException {
    synchronized (lock) {
      for (int i = queues.size(); --i >= 0; ) {
        queues.get(i).put(message);
      }
    }
  }
  
  @Override
  public void dispose() {
    for (WorkerThread thread : threads) {
      thread.terminate();
      thread.joinQuietly();
    }
  }
}
