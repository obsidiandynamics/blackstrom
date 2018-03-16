package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;

public final class RecordBatch implements Iterable<Record> {
  private static final RecordBatch empty = new RecordBatch(Collections.emptyList());
  
  public static RecordBatch empty() { return empty; }
  
  private final List<Record> records;
  
  RecordBatch(List<Record> records) {
    this.records = records;
  }
  
  public boolean isEmpty() {
    return size() == 0;
  }

  public int size() {
    return records.size();
  }
  
  public List<Record> all() {
    final List<Record> list = new ArrayList<>();
    readInto(list);
    return list;
  }
  
  public void readInto(Collection<? super Record> sink) {
    sink.addAll(records);
  }

  @Override
  public Iterator<Record> iterator() {
    return all().iterator();
  }
}
