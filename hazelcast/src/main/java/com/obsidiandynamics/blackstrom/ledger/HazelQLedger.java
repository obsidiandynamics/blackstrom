package com.obsidiandynamics.blackstrom.ledger;

import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.slf4j.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.model.Message;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class HazelQLedger implements Ledger {
  private final HazelcastInstance instance;
  
  private final Logger log;
  
  private final StreamConfig streamConfig;
  
  private final ElectionConfig electionConfig;
  
  private final Publisher publisher;
  
  private final MessageCodec codec;
  
  private final List<Subscriber> allSubscribers = new CopyOnWriteArrayList<>();
  
  private final Map<Integer, Subscriber> groupSubscribers = new ConcurrentHashMap<>();
  
  private final List<Receiver> receivers = new CopyOnWriteArrayList<>();
  
  private final List<ShardedFlow> flows = new CopyOnWriteArrayList<>(); 

  private final AtomicInteger nextHandlerId = new AtomicInteger();
  
  public HazelQLedger(HazelcastInstance instance, HazelQLedgerConfig config) {
    this.instance = instance;
    log = config.getLog();
    streamConfig = config.getStreamConfig();
    electionConfig = config.getElectionConfig();
    codec = config.getCodec();
    final PublisherConfig pubConfig = new PublisherConfig()
        .withLog(log)
        .withStreamConfig(streamConfig);
    publisher = Publisher.createDefault(instance, pubConfig);
  }
  
  @Override
  public void attach(MessageHandler handler) {
    final String group = handler.getGroupId();
    final SubscriberConfig subConfig = new SubscriberConfig()
        .withLog(log)
        .withStreamConfig(streamConfig)
        .withElectionConfig(electionConfig)
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
    final Receiver receiver = subscriber.createReceiver(record -> {
      final DefaultMessageId messageId = new DefaultMessageId(0, record.getOffset());
      final Message message;
      try {
        message = deserialize(record.getData());
      } catch (Exception e) {
        log.error(String.format("Could not decode message at offset %,d", record.getOffset()), e);
        return;
      }
      message.setMessageId(messageId);
      handler.onMessage(context, message);
    });
    receivers.add(receiver);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    try {
      final byte[] bytes = serialize(message);
      publisher.publishAsync(new Record(bytes), (offset, error) -> {
        final MessageId messageId = offset != Record.UNASSIGNED_OFFSET ? new DefaultMessageId(0, offset) : null;
        callback.onAppend(messageId, error);
      });
    } catch (Exception e) {
      callback.onAppend(null, e);
    }
  }
  
  private static final byte[] emptyBytes = {};
  
  private byte[] serialize(Message message) throws Exception {
    final String shardKey = message.getShardKey();
    final byte[] shardKeyBytes = shardKey != null ? shardKey.getBytes() : emptyBytes;
    final byte shardKeyLength = (byte) (shardKeyBytes != null ? shardKeyBytes.length : -1);
    
    final byte[] payload = codec.encode(message);
    final int payloadLength = payload.length;
    
    final int totalLength = 
        1 + shardKeyBytes.length + 
        4 + payload.length;
    final ByteBuffer buf = ByteBuffer.allocate(totalLength);
    buf.put(shardKeyLength);
    buf.put(shardKeyBytes);
    buf.putInt(payloadLength);
    buf.put(payload);
    return buf.array();
  }
  
  private static class DeserializationException extends Exception {
    private static final long serialVersionUID = 1L;
    DeserializationException(String m) { super(m); }
  }
  
  private Message deserialize(byte[] bytes) throws Exception {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    
    final byte shardKeyLength = buf.get();
    final String shardKey;
    if (shardKeyLength != -1) {
      final byte[] shardKeyBytes = new byte[shardKeyLength];
      buf.get(shardKeyBytes);
      shardKey = new String(shardKeyBytes);
    } else {
      shardKey = null;
    }
    
    final int payloadLength = buf.getInt();
    final byte[] payloadBytes = new byte[payloadLength];
    buf.get(payloadBytes);
    if (buf.remaining() != 0) {
      throw new DeserializationException(String.format("%,d unread bytes remaining", buf.remaining()));
    }
    
    final Message message = codec.decode(payloadBytes);
    message.setShardKey(shardKey);
    message.setShard(0);
    return message;
  }
  
  @Override
  public void dispose() {
    final List<Terminable> terminables = new ArrayList<>();
    terminables.add(publisher);
    terminables.addAll(receivers);
    terminables.addAll(allSubscribers);
    terminables.addAll(flows);
    final List<Joinable> joinables = terminables.stream().map(t -> t.terminate()).collect(Collectors.toList());
    joinables.forEach(j -> j.joinQuietly());
  }
}
