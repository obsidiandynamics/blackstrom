package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.stream.*;

import com.obsidiandynamics.blackstrom.model.*;

final class LongList extends ArrayList<Long> {
  private static final long serialVersionUID = 1L;
  
  private LongList(int size) {
    super(size);
  }
  
  LongList plus(long amount) {
    for (int i = 0; i < size(); i++) {
      set(i, get(i) + amount);
    }
    return this;
  }
  
  List<Message> toMessages() {
    return stream().map(i -> new UnknownMessage(String.valueOf(i), 0)).collect(Collectors.toList());
  }
  
  static LongList generate(long startInclusive, long endExclusive) {
    final LongList list = new LongList((int) (endExclusive - startInclusive));
    LongStream.range(startInclusive, endExclusive).forEach(list::add);
    return list;
  }
  
  static LongList empty() {
    return generate(0, 0);
  }
}