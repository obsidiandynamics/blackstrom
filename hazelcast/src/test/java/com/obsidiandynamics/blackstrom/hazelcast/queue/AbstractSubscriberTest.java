package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;

import org.junit.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;

public abstract class AbstractSubscriberTest {
  private HazelcastProvider defaultProvider;
  
  private final Set<HazelcastInstance> instances = new HashSet<>();
  
  private final Set<DefaultSubscriber> subscribers = new HashSet<>();
  
  @Before
  public void before() {
    defaultProvider = new MockHazelcastProvider();
  }
  
  @After
  public void after() {
    subscribers.forEach(s -> s.terminate());
    subscribers.forEach(s -> s.joinQuietly());
    instances.forEach(h -> h.shutdown());
  }
  
  protected HazelcastInstance newInstance() {
    return newInstance(defaultProvider);
  }
  
  protected HazelcastInstance newInstance(HazelcastProvider provider) {
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "none");
    final HazelcastInstance instance = provider.createInstance(config);
    instances.add(instance);
    return instance;
  }
  
  protected DefaultSubscriber configureSubscriber(SubscriberConfig config) {
    return configureSubscriber(newInstance(), config);
  }
  
  protected DefaultSubscriber configureSubscriber(HazelcastInstance instance, SubscriberConfig config) {
    final DefaultSubscriber subscriber = Subscriber.createDefault(instance, config);
    subscribers.add(subscriber);
    return subscriber;
  }
}