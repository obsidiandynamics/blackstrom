package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.obsidiandynamics.blackstrom.worker.*;

public interface Receiver extends Terminable, Joinable {
  @FunctionalInterface
  interface RecordHandler {
    void onRecord(Record record) throws InterruptedException;
  }
}
