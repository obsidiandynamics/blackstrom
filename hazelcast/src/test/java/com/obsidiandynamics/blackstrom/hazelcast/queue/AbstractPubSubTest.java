package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;

public abstract class AbstractPubSubTest {
  protected HazelcastProvider defaultProvider;
  
  protected final Set<HazelcastInstance> instances = new HashSet<>();
  
  protected final Set<Terminable> terminables = new HashSet<>();

  protected final Timesert await = Wait.SHORT;
  
  @Before
  public void before() {
    defaultProvider = new MockHazelcastProvider();
  }
  
  @After
  public void after() {
    final Set<Joinable> joinables = terminables.stream()
        .map(t -> t.terminate()).collect(Collectors.toSet());
    joinables.forEach(s -> s.joinQuietly());
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
  
  protected DefaultPublisher configurePublisher(PublisherConfig config) {
    return configurePublisher(newInstance(), config);
  }
  
  protected DefaultPublisher configurePublisher(HazelcastInstance instance, PublisherConfig config) {
    final DefaultPublisher publisher = Publisher.createDefault(instance, config);
    terminables.add(publisher);
    return publisher;
  }
  
  protected DefaultSubscriber configureSubscriber(SubscriberConfig config) {
    return configureSubscriber(newInstance(), config);
  }
  
  protected DefaultSubscriber configureSubscriber(HazelcastInstance instance, SubscriberConfig config) {
    final DefaultSubscriber subscriber = Subscriber.createDefault(instance, config);
    terminables.add(subscriber);
    return subscriber;
  }
}