package com.obsidiandynamics.blackstrom.ledger;

import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class BlockingQueueUtils {
  private BlockingQueueUtils() {}
  
  public static void put(BlockingQueue<Message> queue, Message message, AppendCallback callback) {
    try {
      queue.put(message);
      callback.onAppend(message.getMessageId(), null);
    } catch (InterruptedException e) {
      callback.onAppend(null, e);
    }
  }
}
