package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.kafka.KafkaReceiver.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class KafkaLedger implements Ledger {
  private static final int POLL_TIMEOUT_MILLIS = 1_000;

  private static final int PIPELINE_MAX_YIELDS = 100;
  private static final int PIPELINE_BACKOFF_MILLIS = 1;
  
  private static final int RETRY_BACKOFF_MILLIS = 100;

  private final Kafka<String, Message> kafka;

  private final String topic;

  private final Logger log;

  private final String codecLocator;

  private final ConsumerPipeConfig consumerPipeConfig;

  private final Producer<String, Message> producer;

  private final ProducerPipe<String, Message> producerPipe;

  private final List<KafkaReceiver<String, Message>> receivers = new CopyOnWriteArrayList<>();

  private final List<ConsumerPipe<String, Message>> consumerPipes = new CopyOnWriteArrayList<>();
  
  private final List<ShardedFlow> flows = new CopyOnWriteArrayList<>(); 
  
  private final boolean printConfig;
  
  private final int attachRetries;
  
  private final WorkerThread retryThread;
  
  private static class RetryTask {
    final Message message;
    final AppendCallback callback;
    
    RetryTask(Message message, AppendCallback callback) {
      this.message = message;
      this.callback = callback;
    }
  }
  
  private final NodeQueue<RetryTask> retryQueue = new NodeQueue<>();
  private final QueueConsumer<RetryTask> retryQueueConsumer = retryQueue.consumer();

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
    printConfig = config.isPrintConfig();
    consumerPipeConfig = config.getConsumerPipeConfig();
    attachRetries = config.getAttachRetries();
    codecLocator = CodecRegistry.register(config.getCodec());
    retryThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withDaemon(true)
                     .withName(KafkaLedger.class.getSimpleName() + "-retry-" + topic))
        .onCycle(this::onRetry)
        .buildAndStart();

    // may be user-specified in config
    final Properties producerDefaults = new PropertiesBuilder()
        .withSystemDefault("batch.size", 1 << 18)
        .withSystemDefault("linger.ms", 1)
        .withSystemDefault("compression.type", "lz4")
        .withSystemDefault("request.timeout.ms", 10_000)
        .withSystemDefault("retry.backoff.ms", 100)
        .build();

    // set by the application — required for correctness (overrides user config)
    final Properties producerOverrides = new PropertiesBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", KafkaMessageSerializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", 0)
        .with("max.block.ms", Long.MAX_VALUE)
        .build();
    
    if (printConfig) kafka.describeProducer(log::info, producerDefaults, producerOverrides);
    producer = kafka.getProducer(producerDefaults, producerOverrides);
    final String producerPipeThreadName = ProducerPipe.class.getSimpleName() + "-" + topic;
    producerPipe = 
        new ProducerPipe<>(config.getProducerPipeConfig(), producer, producerPipeThreadName, log);
  }
  
  private void onRetry(WorkerThread t) throws InterruptedException {
    final RetryTask retryTask = retryQueueConsumer.poll();
    if (retryTask != null) {
      append(retryTask.message, retryTask.callback);
    } else {
      Thread.sleep(RETRY_BACKOFF_MILLIS);
    }
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
    
    // may be user-specified in config
    final Properties consumerDefaults = new PropertiesBuilder()
        .withSystemDefault("session.timeout.ms", 6_000)
        .withSystemDefault("heartbeat.interval.ms", 2_000)
        .withSystemDefault("max.poll.records", 10_000)
        .build();

    // set by the application — required for correctness (overrides user config)
    final Properties consumerOverrides = new PropertiesBuilder()
        .with("group.id", consumerGroupId)
        .with("auto.offset.reset", autoOffsetReset)
        .with("enable.auto.commit", false)
        .with("auto.commit.interval.ms", 0)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", KafkaMessageDeserializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .build();
    
    if (printConfig) kafka.describeConsumer(log::info, consumerDefaults, consumerOverrides);
    final Consumer<String, Message> consumer = kafka.getConsumer(consumerDefaults, consumerOverrides);
    KafkaRetry.run(attachRetries, log, () -> {
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
    });

    final Integer handlerId;
    final ConsumerOffsets consumerOffsets;
    final Retention retention;
    if (groupId != null) {
      handlerId = nextHandlerId.getAndIncrement();
      consumerOffsets = new ConsumerOffsets();
      consumers.put(handlerId, consumerOffsets);
      final ShardedFlow flow = new ShardedFlow();
      retention = flow;
      flows.add(flow);
    } else {
      handlerId = null;
      consumerOffsets = null;
      retention = NopRetention.getInstance();
    }

    final MessageContext context = new DefaultMessageContext(this, handlerId, retention);
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
      for (int yields = 0;;) {
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
        } else if (yields++ < PIPELINE_MAX_YIELDS) {
          Thread.yield();
        } else {
          yields = 0;
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
      } else if (exception instanceof RetriableException) { 
        logException(exception, "Retriable error publishing %s (queuing in background)", record);
        retryQueue.add(new RetryTask(message, callback));
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
    retryThread.terminate();
    receivers.forEach(t -> t.terminate());
    consumerPipes.forEach(t -> t.terminate());
    producerPipe.terminate();
    flows.forEach(t -> t.terminate());
    retryThread.joinQuietly();
    receivers.forEach(t -> t.joinQuietly());
    consumerPipes.forEach(t -> t.joinQuietly());
    producerPipe.joinQuietly();
    flows.forEach(t -> t.joinQuietly());
    CodecRegistry.deregister(codecLocator);
  }
}
