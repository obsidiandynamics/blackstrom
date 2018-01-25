package com.obsidiandynamics.blackstrom.ledger;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;

final class AppendOnlyArray {
  private volatile int nextWriteIndex;
  
  private final AtomicReference<Message>[] array;
  
  AppendOnlyArray(int capacity) {
    array = Cast.from(new AtomicReference[capacity]);
    for (int i = 0; i < capacity; i++) {
      array[i] = new AtomicReference<>();
    }
  }
  
  void add(Message message) {
    final int index = nextWriteIndex;
    array[index].set(message);
    nextWriteIndex = index + 1;
  }
  
  int size() {
    return nextWriteIndex;
  }
  
  Message get(int index) {
    return array[index].get();
  }
}
