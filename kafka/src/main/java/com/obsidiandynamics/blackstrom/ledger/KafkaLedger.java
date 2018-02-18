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
  private static final int POLL_TIMEOUT_MILLIS = 1_000;

  private static final int PIPELINE_BACKOFF_MILLIS = 1;

  private final Kafka<String, Message> kafka;

  private final String topic;

  private final Logger log;

  private final String codecLocator;

  private final ConsumerPipeConfig consumerPipeConfig;

  private final Producer<String, Message> producer;

  private final ProducerPipe<String, Message> producerPipe;

  private final List<KafkaReceiver<String, Message>> receivers = new CopyOnWriteArrayList<>();

  private final List<ConsumerPipe<String, Message>> consumerPipes = new CopyOnWriteArrayList<>();
  
  private final int attachRetries;

  private static class ConsumerOffsets {
    final Object lock = new Object();
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
  }

  /** Maps handler IDs to consumer offsets. */
  private final Map<Integer, ConsumerOffsets> consumers = new ConcurrentHashMap<>();

  private final AtomicInteger nextHandlerId = new AtomicInteger();

  public KafkaLedger(KafkaLedgerConfig config) {
    kafka = config.getKafka();
    topic = config.getTopic();
    log = config.getLog();
    consumerPipeConfig = config.getConsumerPipeConfig();
    attachRetries = config.getAttachRetries();
    codecLocator = CodecRegistry.register(config.getCodec());

    final Properties props = new PropertiesBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", KafkaMessageSerializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", Integer.MAX_VALUE)
        .with("batch.size", 1 << 18)
        .with("linger.ms", 1)
        .with("compression.type", "lz4")
        .build();
    producer = kafka.getProducer(props);
    final String producerPipeThreadName = ProducerPipe.class.getSimpleName() + "-" + topic;
    producerPipe = 
        new ProducerPipe<>(config.getProducerPipeConfig(), producer, producerPipeThreadName, log);
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
        .with("max.poll.records", 10_000)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", KafkaMessageDeserializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .build();
    final Consumer<String, Message> consumer = kafka.getConsumer(props);
    for (int triesLeft = attachRetries; triesLeft > 0; triesLeft--) {
      try {
        if (groupId != null) {
          consumer.subscribe(Collections.singletonList(topic));
          log.debug("subscribed to topic {}", topic);
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
        break;
      } catch (KafkaException e) {
        if (triesLeft == 1) {
          log.error("Error", e);
          throw e;
        } else {
          log.warn("Error: {} ({} tries remaining)", e, triesLeft - 1);
        }
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
    final String consumerPipeThreadName = ConsumerPipe.class.getSimpleName() + "-" + groupId;
    final RecordHandler<String, Message> pipelinedRecordHandler = records -> {
      for (ConsumerRecord<String, Message> record : records) {
        final DefaultMessageId messageId = new DefaultMessageId(record.partition(), record.offset());
        final Message message = record.value();
        message.setMessageId(messageId);
        message.setShardKey(record.key());
        message.setShard(record.partition());
        handler.onMessage(context, message);
      }
    };
    final ConsumerPipe<String, Message> consumerPipe = 
        new ConsumerPipe<>(consumerPipeConfig, pipelinedRecordHandler, consumerPipeThreadName);
    consumerPipes.add(consumerPipe);
    final RecordHandler<String, Message> recordHandler = records -> {
      for (;;) {
        final boolean enqueued = consumerPipe.receive(records);

        if (consumerOffsets != null) {
          final Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot;
          synchronized (consumerOffsets.lock) {
            if (! consumerOffsets.offsets.isEmpty()) {
              offsetsSnapshot = consumerOffsets.offsets;
              consumerOffsets.offsets = new HashMap<>(offsetsSnapshot.size());
            } else {
              offsetsSnapshot = null;
            }
          }

          if (offsetsSnapshot != null) {
            log.trace("Committing offsets {}", offsetsSnapshot);
            consumer.commitAsync(offsetsSnapshot, 
                                 (offsets, exception) -> logException(exception, "Error committing offsets %s", offsets));
          }
        }

        if (enqueued) {
          break;
        } else {
          Thread.sleep(PIPELINE_BACKOFF_MILLIS);
        }
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
    final Callback sendCallback = (metadata, exception) -> {
      if (exception == null) {
        callback.onAppend(new DefaultMessageId(metadata.partition(), metadata.offset()), null);
      } else {
        callback.onAppend(null, exception);
        logException(exception, "Error publishing %s", record);
      }
    };

    producerPipe.send(record, sendCallback);
  }

  @Override
  public void confirm(Object handlerId, MessageId messageId) {
    if (handlerId != null) {
      final ConsumerOffsets consumer = consumers.get(handlerId);
      final DefaultMessageId defaultMessageId = (DefaultMessageId) messageId;
      final TopicPartition tp = new TopicPartition(topic, defaultMessageId.getShard());
      final OffsetAndMetadata om = new OffsetAndMetadata(defaultMessageId.getOffset());
      synchronized (consumer.lock) {
        consumer.offsets.put(tp, om);
      }
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
    consumerPipes.forEach(t -> t.terminate());
    producerPipe.terminate();
    receivers.forEach(t -> t.joinQuietly());
    consumerPipes.forEach(t -> t.joinQuietly());
    producerPipe.joinQuietly();
    CodecRegistry.deregister(codecLocator);
  }
}
