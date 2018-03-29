package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.slf4j.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.model.Message;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.blackstrom.worker.Terminator;

public final class HazelQLedger implements Ledger {
  private final HazelcastInstance instance;
  
  private final HazelQLedgerConfig config;
  
  private final Publisher publisher;
  
  private final MessageCodec codec;
  
  private final List<Subscriber> allSubscribers = new ArrayList<>();
  
  private final Map<Integer, Subscriber> groupSubscribers = new HashMap<>();
  
  private final List<Receiver> receivers = new ArrayList<>();
  
  private final List<ShardedFlow> flows = new ArrayList<>(); 

  private final AtomicInteger nextHandlerId = new AtomicInteger();
  
  public HazelQLedger(HazelcastInstance instance, HazelQLedgerConfig config) {
    this.instance = instance;
    this.config = config;
    codec = config.getCodec();
    final PublisherConfig pubConfig = new PublisherConfig()
        .withLog(config.getLog())
        .withStreamConfig(config.getStreamConfig());
    publisher = Publisher.createDefault(instance, pubConfig);
  }
  
  @Override
  public void attach(MessageHandler handler) {
    final String group = handler.getGroupId();
    final SubscriberConfig subConfig = new SubscriberConfig()
        .withLog(config.getLog())
        .withStreamConfig(config.getStreamConfig())
        .withElectionConfig(config.getElectionConfig())
        .withGroup(group);
    final Subscriber subscriber = Subscriber.createDefault(instance, subConfig);
    allSubscribers.add(subscriber);

    final Integer handlerId;
    final Retention retention;
    if (group != null) {
      handlerId = nextHandlerId.getAndIncrement();
      groupSubscribers.put(handlerId, subscriber);
      final ShardedFlow flow = new ShardedFlow();
      retention = flow;
      flows.add(flow);
    } else {
      handlerId = null;
      retention = NopRetention.getInstance();
    }

    final MessageContext context = new DefaultMessageContext(this, handlerId, retention);
    final Receiver receiver = subscriber.createReceiver(record -> receive(codec, record, config.getLog(), handler, context), 
                                                        config.getPollInterval());
    receivers.add(receiver);
  }
  
  static void receive(MessageCodec codec, Record record, Logger log, MessageHandler handler, MessageContext context) {
    final DefaultMessageId messageId = new DefaultMessageId(0, record.getOffset());
    final Message message;
    try {
      message = MessagePacker.unpack(codec, record.getData());
    } catch (Exception e) {
      log.error(String.format("Could not decode message at offset %,d", record.getOffset()), e);
      return;
    }
    message.setMessageId(messageId);
    handler.onMessage(context, message);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    appendWithCallback(codec, publisher, message, callback);
  }
  
  static void appendWithCallback(MessageCodec codec, Publisher publisher, Message message, AppendCallback callback) {
    final byte[] bytes;
    try {
      bytes = MessagePacker.pack(codec, message);
    } catch (Exception e) {
      callback.onAppend(null, e);
      return;
    }
    
    publisher.publishAsync(new Record(bytes), (offset, error) -> {
      final MessageId messageId = offset != Record.UNASSIGNED_OFFSET ? new DefaultMessageId(0, offset) : null;
      callback.onAppend(messageId, error);
    });
  }
  
  @Override
  public void confirm(Object handlerId, MessageId messageId) {
    final Subscriber subscriber = groupSubscribers.get(handlerId);
    final DefaultMessageId defaultMessageId = (DefaultMessageId) messageId;
    subscriber.confirm(defaultMessageId.getOffset());
  }
  
  @Override
  public void dispose() {
    Terminator.blank()
    .add(publisher)
    .add(receivers)
    .add(allSubscribers)
    .add(flows)
    .terminate()
    .joinSilently();
  }
}
