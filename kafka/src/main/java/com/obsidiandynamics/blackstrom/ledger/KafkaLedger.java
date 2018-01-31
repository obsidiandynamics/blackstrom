package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.kafka.KafkaReceiver.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaLedger implements Ledger {
  private static final long POLL_TIMEOUT_MILLIS = 1_000;
  
  private Logger log = LoggerFactory.getLogger(KafkaLedger.class);
  
  private final Kafka<String, Message> kafka;
  
  private final String topic;
  
  private final Producer<String, Message> producer;
  
  private final Object producerLock = new Object();
  
  private boolean producerDisposed;
  
  private final List<KafkaReceiver<String, Message>> receivers = new CopyOnWriteArrayList<>();
  
  private static class ConsumerOffsets {
    final Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
  }
  
  /** Maps handler IDs to consumer offsets. */
  private final Map<Integer, ConsumerOffsets> consumers = new ConcurrentHashMap<>();
  
  private final AtomicInteger nextHandlerId = new AtomicInteger();
  
  private final boolean mapPayload;
  
  public KafkaLedger(Kafka<String, Message> kafka, String topic, boolean mapPayload) {
    this.kafka = kafka;
    this.topic = topic;
    this.mapPayload = mapPayload;
    
    final Properties props = new PropertiesBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", KafkaJacksonMessageSerializer.class.getName())
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", Integer.MAX_VALUE)
        .with("batch.size", 1 << 16)
        .with("linger.ms", 0)
        .with("buffer.memory", 33_554_432)
        .with("compression.type", "lz4")
        .build();
    producer = kafka.getProducer(props);
  }
  
  public KafkaLedger withLogger(Logger log) {
    this.log = log;
    return this;
  }

  @Override
  public void attach(MessageHandler handler) {
    final String groupId = handler.getGroupId();
    final String consumerGroupId;
    final String autoOffsetReset;
    if (groupId != null) {
      consumerGroupId = groupId;
      autoOffsetReset = OffsetResetStrategy.EARLIEST.name().toLowerCase();
    } else {
      consumerGroupId = null;
      autoOffsetReset = OffsetResetStrategy.LATEST.name().toLowerCase();
    }
    
    final Properties props = new PropertiesBuilder()
        .with("group.id", consumerGroupId)
        .with("auto.offset.reset", autoOffsetReset)
        .with("enable.auto.commit", false)
        .with("auto.commit.interval.ms", 0)
        .with("session.timeout.ms", 6_000)
        .with("heartbeat.interval.ms", 2_000)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", KafkaJacksonMessageDeserializer.class.getName())
        .with(KafkaJacksonMessageDeserializer.CONFIG_MAP_PAYLOAD, mapPayload)
        .build();
    final Consumer<String, Message> consumer = kafka.getConsumer(props);
    if (groupId != null) {
      consumer.subscribe(Collections.singletonList(topic));
    } else {
      final List<PartitionInfo> infos = consumer.partitionsFor(topic);
      final List<TopicPartition> partitions = infos.stream()
          .map(i -> new TopicPartition(i.topic(), i.partition()))
          .collect(Collectors.toList());
      log.debug("infos={}, partitions={}", infos, partitions);
      final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
      consumer.assign(partitions);
      for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
        consumer.seek(entry.getKey(), entry.getValue());
      }
    }
    
    final Integer handlerId;
    final ConsumerOffsets consumerOffsets;
    if (groupId != null) {
      handlerId = nextHandlerId.getAndIncrement();
      consumerOffsets = new ConsumerOffsets();
      consumers.put(handlerId, consumerOffsets);
    } else {
      handlerId = null;
      consumerOffsets = null;
    }
    
    final MessageContext context = new DefaultMessageContext(this, handlerId);
    
    final RecordHandler<String, Message> recordHandler = records -> {
      for (ConsumerRecord<String, Message> record : records) {
        final KafkaMessageId messageId = KafkaMessageId.fromRecord(record);
        final Message message = record.value();
        message.setMessageId(messageId);
        message.setShardKey(record.key());
        message.setShard(record.partition());
        handler.onMessage(context, message);
      }
      
      if (consumerOffsets != null && ! consumerOffsets.offsets.isEmpty()) {
        consumer.commitAsync(consumerOffsets.offsets, 
                             (offsets, exception) -> logException(exception, "Error committing offsets %s", consumerOffsets.offsets));
        consumerOffsets.offsets.clear();
      }
    };
    
    final String threadName = KafkaLedger.class.getSimpleName() + "-receiver-" + groupId;
    final KafkaReceiver<String, Message> receiver = new KafkaReceiver<>(consumer, POLL_TIMEOUT_MILLIS, 
        threadName, recordHandler, KafkaReceiver.genericErrorLogger(log));
    receivers.add(receiver);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    final ProducerRecord<String, Message> record = 
        new ProducerRecord<>(topic, message.getShardIfAssigned(), message.getShardKey(), message);
    synchronized (producerLock) {
      if (! producerDisposed) {
        producer.send(record, 
                      (metadata, exception) -> {
                        if (exception == null) {
                          callback.onAppend(new KafkaMessageId(topic, metadata.partition(), metadata.offset()), null);
                        } else {
                          callback.onAppend(null, exception);
                          logException(exception, "Error publishing %s", record);
                        }
                      });
      }
    }
  }

  @Override
  public void confirm(Object handlerId, MessageId messageId) {
    if (handlerId != null) {
      final ConsumerOffsets consumer = consumers.get(handlerId);
      final KafkaMessageId kafkaMessageId = (KafkaMessageId) messageId;
      consumer.offsets.put(new TopicPartition(topic, kafkaMessageId.getPartition()), 
                           new OffsetAndMetadata(kafkaMessageId.getOffset()));
    }
  }
  
  private void logException(Exception cause, String messageFormat, Object... messageArgs) {
    if (cause != null) {
      log.warn(String.format(messageFormat, messageArgs), cause);
    }
  }

  @Override
  public void dispose() {
    receivers.forEach(t -> t.terminate());
    synchronized (producerLock) {
      producer.close();
      producerDisposed = true;
    }
    receivers.forEach(t -> t.joinQuietly());
  }
}
