package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class PubSubTest extends AbstractPubSubTest {
  private final int SCALE = Testmark.getOptions(Scale.class, Scale.UNITY).magnitude();
  
  @Test
  public void testOneWay() {
    testOneWay(2, 4, 10_000 * SCALE);
  }
  
  private void testOneWay(int producers, int consumers, int messagesPerProducer) {
    
  }
}
