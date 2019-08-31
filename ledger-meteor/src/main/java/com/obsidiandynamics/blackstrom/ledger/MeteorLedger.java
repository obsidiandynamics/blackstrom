package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.model.Message;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.format.*;
import com.obsidiandynamics.meteor.*;
import com.obsidiandynamics.worker.Terminator;
import com.obsidiandynamics.zerolog.*;

public final class MeteorLedger implements Ledger {
  private final HazelcastInstance instance;
  
  private final MeteorLedgerConfig config;
  
  private final Publisher publisher;
  
  private final MessageCodec codec;
  
  private final List<Subscriber> allSubscribers = new CopyOnWriteArrayList<>();
  
  private final Map<Integer, Subscriber> groupSubscribers = new ConcurrentHashMap<>();
  
  private final AtomicInteger nextHandlerId = new AtomicInteger();
  
  public MeteorLedger(HazelcastInstance instance, MeteorLedgerConfig config) {
    mustExist(config, "Config cannot be null").validate();
    this.instance = mustExist(instance, "Hazelcast instance cannot be null");
    this.config = config;
    codec = config.getCodec();
    final PublisherConfig pubConfig = new PublisherConfig()
        .withZlg(config.getZlg())
        .withStreamConfig(config.getStreamConfig());
    publisher = Publisher.createDefault(instance, pubConfig);
  }
  
  @Override
  public Object attach(MessageHandler handler) {
    final String groupId = handler.getGroupId();
    final SubscriberConfig subConfig = new SubscriberConfig()
        .withZlg(config.getZlg())
        .withStreamConfig(config.getStreamConfig())
        .withElectionConfig(config.getElectionConfig())
        .withGroup(groupId);
    final Subscriber subscriber = Subscriber.createDefault(instance, subConfig);
    allSubscribers.add(subscriber);

    final Integer handlerId;
    final Retention retention;
    if (groupId != null) {
      handlerId = nextHandlerId.getAndIncrement();
      groupSubscribers.put(handlerId, subscriber);
      final ShardedFlow flow = new ShardedFlow();
      retention = flow;
    } else {
      handlerId = null;
      retention = NopRetention.getInstance();
    }

    final MessageContext context = new DefaultMessageContext(this, handlerId, retention);
    subscriber.attachReceiver(record -> receive(codec, record, config.getZlg(), handler, context), 
                              config.getPollInterval());
    return handlerId;
  }
  
  static void receive(MessageCodec codec, Record record, Zlg zlg, MessageHandler handler, MessageContext context) {
    final DefaultMessageId messageId = new DefaultMessageId(0, record.getOffset());
    final Message message;
    try {
      message = MessagePacker.unpack(codec, record.getData());
    } catch (Exception e) {
      zlg.e("Could not decode message at offset %,d\n%s", 
            z -> z.arg(record::getOffset).arg(Args.map(record::getData, Binary::dump)).threw(e));
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
  public boolean isAssigned(Object handlerId, int shard) {
    return handlerId == null || groupSubscribers.get(handlerId).isAssigned();
  }
  
  @Override
  public void dispose() {
    Terminator.blank()
    .add(publisher)
    .add(allSubscribers)
    .terminate()
    .joinSilently();
  }
}
