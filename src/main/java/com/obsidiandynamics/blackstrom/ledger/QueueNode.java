package com.obsidiandynamics.blackstrom.ledger;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.model.*;

final class QueueNode extends AtomicReference<QueueNode> {
  private static final long serialVersionUID = 1L;

  final Message m;

  QueueNode(Message m) { this.m = m; }
  
  static QueueNode anchor() {
    return new QueueNode(null);
  }
  
  void appendTo(AtomicReference<QueueNode> tail) {
    final QueueNode t1 = tail.getAndSet(this);
    t1.lazySet(this);
  }
}