package com.obsidiandynamics.blackstrom.hazelcast;

import java.util.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.test.*;

public final class HazelcastSampleTester {
  public static void main(String[] args) {
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "slf4j");
    
    final HazelcastInstanceFactory instanceFactory = 
        new TestHazelcastInstanceFactory()::newHazelcastInstance;
    
    final HazelcastInstance h0 = instanceFactory.create(config);
    final HazelcastInstance h1 = instanceFactory.create(config);
    useInstance(h0);
    useInstance(h1);
    h0.shutdown();
    h1.shutdown();
  }
  
  private static void useInstance(HazelcastInstance instance) {
    final Map<Integer, String> mapCustomers = instance.getMap("customers");
    mapCustomers.put(1, "Joe");
    mapCustomers.put(2, "Ali");
    mapCustomers.put(3, "Avi");

    System.out.println("Customer with key 1: "+ mapCustomers.get(1));
    System.out.println("Map Size: " + mapCustomers.size());

    final Queue<String> queueCustomers = instance.getQueue("customers");
    queueCustomers.offer("Tom");
    queueCustomers.offer("Mary");
    queueCustomers.offer("Jane");
    System.out.println("First customer: " + queueCustomers.poll());
    System.out.println("Second customer: " + queueCustomers.peek());
    System.out.println("Queue size: " + queueCustomers.size());
  }
}
