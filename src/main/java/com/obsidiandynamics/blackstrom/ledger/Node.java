package com.obsidiandynamics.blackstrom.ledger;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.model.*;

final class Node extends AtomicReference<Node> {
  private static final long serialVersionUID = 1L;

  final Message m;

  Node(Message m) { this.m = m; }
  
  static Node anchor() {
    return new Node(null);
  }
  
  void appendTo(AtomicReference<Node> tail) {
    final Node t1 = tail.getAndSet(this);
    t1.lazySet(this);
  }
}