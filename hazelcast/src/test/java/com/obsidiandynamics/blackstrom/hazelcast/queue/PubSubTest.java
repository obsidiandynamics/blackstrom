package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.worker.*;

public final class PubSubTest {
  private final Set<Publisher> publishers = new HashSet<>();
  
  @After
  public void after() {
    final Set<Joinable> publishersJoin = publishers.stream().map(p -> p.terminate()).collect(Collectors.toSet());
    publishersJoin.forEach(p -> p.joinQuietly());
  }

  @Test
  public void testPubSubNoGroup() {
    //TODO
  }

}
