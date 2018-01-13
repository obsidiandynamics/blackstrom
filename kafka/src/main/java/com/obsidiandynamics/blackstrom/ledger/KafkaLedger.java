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
  
  private final List<KafkaReceiver<String, Message>> receivers = new CopyOnWriteArrayList<>();
  
  private final Map<Integer, Consumer<String, Message>> consumers = new ConcurrentHashMap<>();
  
  private final AtomicInteger nextHandlerId = new AtomicInteger();
  
  public KafkaLedger(Kafka<String, Message> kafka, String topic) {
    this.kafka = kafka;
    this.topic = topic;
    
    final Properties props = new PropertiesBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", KafkaJacksonMessageSerializer.class.getName())
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", Integer.MAX_VALUE)
        .with("batch.size", 16_384)
        .with("linger.ms", 0)
        .with("buffer.memory", 33_554_432)
        .with("compression.type", "snappy")
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
        .with(KafkaJacksonMessageDeserializer.CONFIG_MAP_PAYLOAD, String.valueOf(false))
        .build();
    final Consumer<String, Message> consumer = kafka.getConsumer(props);
    if (groupId != null) {
      consumer.subscribe(Collections.singletonList(topic));
    } else {
      final List<PartitionInfo> infos = consumer.partitionsFor(topic);
      final List<TopicPartition> partitions = infos.stream()
          .map(i -> new TopicPartition(i.topic(), i.partition()))
          .collect(Collectors.toList());
      final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
      consumer.assign(partitions);
      for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
        consumer.seek(entry.getKey(), entry.getValue());
      }
    }
    
    final Integer handlerId = groupId != null ? nextHandlerId.getAndIncrement() : null;
    final MessageContext context = new DefaultMessageContext(this, handlerId);
    
    final RecordHandler<String, Message> recordHandler = records -> {
      for (ConsumerRecord<String, Message> record : records) {
        final KafkaMessageId messageId = KafkaMessageId.fromRecord(record);
        final Message message = record.value();
        message.withMessageId(messageId).withShardKey(record.key());
        handler.onMessage(context, message);
      }
    };
    
    final String threadName = getClass().getSimpleName() + "-receiver-" + groupId;
    final KafkaReceiver<String, Message> receiver = new KafkaReceiver<>(consumer, POLL_TIMEOUT_MILLIS, 
        threadName, recordHandler, KafkaReceiver.genericErrorLogger(log));
    receivers.add(receiver);
    
    if (handlerId != null) {
      consumers.put(handlerId, consumer);
    }
  }

  @Override
  public void append(Message message) throws Exception {
    final ProducerRecord<String, Message> record = new ProducerRecord<>(topic, message.getShardKey(), message);
    producer.send(record, 
                  (metadata, exception) -> logException(exception, "Error publishing %s", record));
  }

  @Override
  public void confirm(Object handlerId, Object messageId) {
    if (handlerId != null) {
      final Consumer<?, ?> consumer = consumers.get(handlerId);
      final KafkaMessageId kafkaMessageId = (KafkaMessageId) messageId;
      consumer.commitAsync(kafkaMessageId.toOffset(), 
                           (offsets, exception) -> logException(exception, "Error commiting %s", messageId));
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
    receivers.forEach(t -> t.joinQuietly());
  }
}
