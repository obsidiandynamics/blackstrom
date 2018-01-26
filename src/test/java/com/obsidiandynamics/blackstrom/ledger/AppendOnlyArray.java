package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.model.*;

final class AppendOnlyArray {
  private volatile int nextWriteIndex;
  
  private final Message[] array;
  
  AppendOnlyArray(int capacity) {
    array = new Message[capacity];
  }
  
  void add(Message message) {
    final int index = nextWriteIndex;
    array[index] = message;
    nextWriteIndex = index + 1; // volatile piggyback -- barrier write
  }
  
  int size() {
    return nextWriteIndex;
  }
  
  Message get(int index) {
    // assumes the reader has already checked the value of nextWriteIndex, so writes to the array
    // with index < nextWriteIndex would be visible to the reader
    return array[index];
  }
}