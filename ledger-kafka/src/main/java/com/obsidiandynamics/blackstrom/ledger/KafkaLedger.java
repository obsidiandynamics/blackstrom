package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.jackdaw.AsyncReceiver.*;
import com.obsidiandynamics.nodequeue.*;
import com.obsidiandynamics.retry.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaLedger implements Ledger {
  private static final int POLL_TIMEOUT_MILLIS = 1_000;
  private static final int PIPELINE_BACKOFF_MILLIS = 1;
  private static final int RETRY_BACKOFF_MILLIS = 100;
  private static final long OFFSET_DRAIN_CHECK_INTERVAL_MILLIS = 10;

  private final Kafka<String, Message> kafka;

  private final String topic;

  private final Zlg zlg;

  private final String codecLocator;

  private final ConsumerPipeConfig consumerPipeConfig;
  
  private final int maxConsumerPipeYields;

  private final Producer<String, Message> producer;

  private final ProducerPipe<String, Message> producerPipe;

  private final List<AsyncReceiver<String, Message>> receivers = new ArrayList<>();

  private final List<ConsumerPipe<String, Message>> consumerPipes = new ArrayList<>();
  
  private final List<ShardedFlow> flows = new ArrayList<>(); 
  
  private final boolean printConfig;
  
  private final int ioRetries;
  
  private final boolean drainConfirmations;
  
  private final WorkerThread retryThread;
  
  private static final class RetryTask {
    final Message message;
    final AppendCallback callback;
    
    RetryTask(Message message, AppendCallback callback) {
      this.message = message;
      this.callback = callback;
    }
  }
  
  private final NodeQueue<RetryTask> retryQueue = new NodeQueue<>();
  private final QueueConsumer<RetryTask> retryQueueConsumer = retryQueue.consumer();

  static final class ConsumerState {
    final Object lock = new Object();
    
    Map<TopicPartition, OffsetAndMetadata> offsetsConfirmed = new HashMap<>();
    Map<Integer, Long> offsetsPending = new HashMap<>();
    Map<Integer, Long> offsetsAccepted = new HashMap<>();
    Set<Integer> assignedPartitions = Collections.emptySet();
  }

  /** Maps handler IDs to consumer offsets. */
  private final Map<Integer, ConsumerState> consumerStates = new HashMap<>();

  private final AtomicInteger nextHandlerId = new AtomicInteger();

  public KafkaLedger(KafkaLedgerConfig config) {
    kafka = config.getKafka();
    topic = config.getTopic();
    zlg = config.getZlg();
    printConfig = config.isPrintConfig();
    consumerPipeConfig = config.getConsumerPipeConfig();
    maxConsumerPipeYields = config.getMaxConsumerPipeYields();
    ioRetries = config.getIORetries();
    drainConfirmations = config.isDrainConfirmations();
    codecLocator = CodecRegistry.register(config.getCodec());
    retryThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(KafkaLedger.class, "retry", topic))
        .onCycle(this::onRetry)
        .buildAndStart();

    // may be user-specified in config
    final Properties producerDefaults = new PropsBuilder()
        .withSystemDefault("batch.size", 1 << 18)
        .withSystemDefault("linger.ms", 1)
        .withSystemDefault("compression.type", "lz4")
        .withSystemDefault("request.timeout.ms", 10_000)
        .withSystemDefault("retry.backoff.ms", 100)
        .build();

    // set by the application — required for correctness (overrides user config)
    final Properties producerOverrides = new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", KafkaMessageSerializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", 0)
        .with("max.block.ms", Long.MAX_VALUE)
        .build();
    
    if (printConfig) kafka.describeProducer(zlg::i, producerDefaults, producerOverrides);
    producer = kafka.getProducer(producerDefaults, producerOverrides);
    final String producerPipeThreadName = ProducerPipe.class.getSimpleName() + "-" + topic;
    producerPipe = 
        new ProducerPipe<>(config.getProducerPipeConfig(), producer, producerPipeThreadName, zlg::w);
  }
  
  private void onRetry(WorkerThread t) throws InterruptedException {
    final RetryTask retryTask = retryQueueConsumer.poll();
    if (retryTask != null) {
      append(retryTask.message, retryTask.callback);
    } else {
      Thread.sleep(RETRY_BACKOFF_MILLIS);
    }
  }
  
  private static Map<TopicPartition, OffsetAndMetadata> toKafkaOffsetsMap(Map<Integer, Long> sourceMap, String topic) {
    final Map<TopicPartition, OffsetAndMetadata> targetMap = new HashMap<>();
    for (Map.Entry<Integer, Long> entry : sourceMap.entrySet()) {
      targetMap.put(new TopicPartition(topic, entry.getKey()), new OffsetAndMetadata(entry.getValue()));
    }
    return targetMap;
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
    final Properties consumerDefaults = new PropsBuilder()
        .withSystemDefault("session.timeout.ms", 6_000)
        .withSystemDefault("heartbeat.interval.ms", 2_000)
        .withSystemDefault("max.poll.records", 10_000)
        .build();

    // set by the application — required for correctness (overrides user config)
    final Properties consumerOverrides = new PropsBuilder()
        .with("group.id", consumerGroupId)
        .with("auto.offset.reset", autoOffsetReset)
        .with("enable.auto.commit", false)
        .with("auto.commit.interval.ms", 0)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", KafkaMessageDeserializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .build();
    
    if (printConfig) kafka.describeConsumer(zlg::i, consumerDefaults, consumerOverrides);
    final Consumer<String, Message> consumer = kafka.getConsumer(consumerDefaults, consumerOverrides);
    final ConsumerState consumerState;
    if (groupId != null) {
      consumerState = new ConsumerState();
    } else {
      consumerState = null;
    }
    
    new Retry()
    .withAttempts(ioRetries)
    .withFaultHandler(zlg::w)
    .withErrorHandler(zlg::e)
    .run(() -> {
      if (groupId != null) {
        final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            synchronized (consumerState.lock) {
              consumerState.assignedPartitions = Collections.emptySet();
            }

            if (drainConfirmations) {
              // blocks until all pending offsets have been confirmed, and performs a sync commit
              drainOffsets(topic, consumer, consumerState, ioRetries, sleepFor(OFFSET_DRAIN_CHECK_INTERVAL_MILLIS), zlg);
            }
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            final Set<Integer> assignedPartitions = partitions.stream().map(TopicPartition::partition).collect(Collectors.toSet());
            synchronized (consumerState.lock) {
              consumerState.assignedPartitions = assignedPartitions;
            }
          }
        };
        consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
        zlg.d("Subscribed to topic %s", z -> z.arg(topic));
      } else {
        final List<PartitionInfo> infos = consumer.partitionsFor(topic);
        final List<TopicPartition> partitions = infos.stream()
            .map(i -> new TopicPartition(i.topic(), i.partition()))
            .collect(Collectors.toList());
        zlg.d("infos=%s, partitions=%s", z -> z.arg(infos).arg(partitions));
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        consumer.assign(partitions);
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
          consumer.seek(entry.getKey(), entry.getValue());
        }
      }
    });

    final Integer handlerId;
    final Retention retention;
    if (groupId != null) {
      handlerId = nextHandlerId.getAndIncrement();
      consumerStates.put(handlerId, consumerState);
      final ShardedFlow flow = new ShardedFlow();
      retention = flow;
      flows.add(flow);
    } else {
      handlerId = null;
      retention = NopRetention.getInstance();
    }

    final MessageContext context = new DefaultMessageContext(this, handlerId, retention);
    final String consumerPipeThreadName = ConsumerPipe.class.getSimpleName() + "-" + groupId;
    final RecordHandler<String, Message> pipelinedRecordHandler = records -> {
      for (ConsumerRecord<String, Message> record : records) {
        handleRecord(handler, consumerState, context, record, zlg);
      }
    };
    final ConsumerPipe<String, Message> consumerPipe = 
        new ConsumerPipe<>(consumerPipeConfig, pipelinedRecordHandler, consumerPipeThreadName);
    consumerPipes.add(consumerPipe);
    final RecordHandler<String, Message> recordHandler = records -> {
      for (int yields = 0;;) {
        final boolean enqueued = consumerPipe.receive(records);

        if (consumerState != null) {
          commitOffsets(consumer, consumerState, zlg);
        }

        if (enqueued) {
          break;
        } else if (yields < maxConsumerPipeYields) {
          yields++;
          Thread.yield();
        } else {
          Thread.sleep(PIPELINE_BACKOFF_MILLIS);
        }
      }
    };

    final String threadName = KafkaLedger.class.getSimpleName() + "-receiver-" + groupId;
    final AsyncReceiver<String, Message> receiver = new AsyncReceiver<>(consumer, POLL_TIMEOUT_MILLIS, 
        threadName, recordHandler, zlg::w);
    receivers.add(receiver);
  }
  
  static Runnable sleepFor(long sleepMillis) {
    return () -> Threads.sleep(sleepMillis);
  }

  static void drainOffsets(String topic, Consumer<String, Message> consumer, ConsumerState consumerState, 
                           int ioRetries, Runnable intervalSleep, Zlg zlg) {
    for (;;) {
      final boolean allPendingOffsetsConfirmed;
      final Map<Integer, Long> offsetsAcceptedSnapshot;
      synchronized (consumerState.lock) {
        allPendingOffsetsConfirmed = consumerState.offsetsPending.isEmpty();
        if (allPendingOffsetsConfirmed) {
          offsetsAcceptedSnapshot = consumerState.offsetsAccepted;
          consumerState.offsetsAccepted = new HashMap<>();
        } else {
          zlg.d("Offsets pending: %s", z -> z.arg(consumerState.offsetsPending));
          offsetsAcceptedSnapshot = null;
        }
      }

      if (allPendingOffsetsConfirmed) {
        zlg.d("All offsets confirmed: %s", z -> z.arg(offsetsAcceptedSnapshot));
        new Retry()
        .withAttempts(ioRetries)
        .withFaultHandler(zlg::w)
        .withErrorHandler(zlg::e)
        .run(() -> consumer.commitSync(toKafkaOffsetsMap(offsetsAcceptedSnapshot, topic)));
        return;
      } else {
        intervalSleep.run();
      }
    }
  }

  static void commitOffsets(Consumer<String, Message> consumer, ConsumerState consumerState, Zlg zlg) {
    final Map<TopicPartition, OffsetAndMetadata> confirmedSnapshot;
    synchronized (consumerState.lock) {
      if (! consumerState.offsetsConfirmed.isEmpty()) {
        confirmedSnapshot = consumerState.offsetsConfirmed;
        consumerState.offsetsConfirmed = new HashMap<>(confirmedSnapshot.size());
      } else {
        confirmedSnapshot = null;
      }
    }

    if (confirmedSnapshot != null) {
      zlg.t("Committing offsets %s", z -> z.arg(confirmedSnapshot));
      consumer.commitAsync(confirmedSnapshot, 
                           (offsets, exception) -> logException(zlg, exception, "Error committing offsets %s", offsets));
    }
  }

  static void handleRecord(MessageHandler handler, ConsumerState consumerState, MessageContext context,
                           ConsumerRecord<String, Message> record, Zlg zlg) {
    final boolean accepted;
    if (consumerState != null) {
      synchronized (consumerState.lock) {
        accepted = consumerState.assignedPartitions.contains(record.partition());
        if (accepted) {
          consumerState.offsetsPending.put(record.partition(), record.offset());
          consumerState.offsetsAccepted.put(record.partition(), record.offset());
        } else {
          zlg.d("Skipping message at offset %d: partition %d has been reassigned", z -> z.arg(record::offset).arg(record::partition));
        }
      }
    } else {
      accepted = true;
    }
    
    if (accepted) {
      final DefaultMessageId messageId = new DefaultMessageId(record.partition(), record.offset());
      final Message message = record.value();
      message.setMessageId(messageId);
      message.setShardKey(record.key());
      message.setShard(record.partition());
      handler.onMessage(context, message);
    }
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    final ProducerRecord<String, Message> record = 
        new ProducerRecord<>(topic, message.getShardIfAssigned(), message.getShardKey(), message);
    final Callback sendCallback = (metadata, exception) -> {
      if (exception == null) {
        callback.onAppend(new DefaultMessageId(metadata.partition(), metadata.offset()), null);
      } else if (exception instanceof RetriableException) { 
        logException(zlg, exception, "Retriable error publishing %s (queuing in background)", record);
        retryQueue.add(new RetryTask(message, callback));
      } else {
        callback.onAppend(null, exception);
        logException(zlg, exception, "Error publishing %s", record);
      }
    };

    producerPipe.send(record, sendCallback);
  }

  @Override
  public void confirm(Object handlerId, MessageId messageId) {
    final ConsumerState consumerState = consumerStates.get(handlerId);
    confirm((DefaultMessageId) messageId, consumerState, topic, zlg);
  }

  static void confirm(DefaultMessageId messageId, ConsumerState consumerState, String topic, Zlg zlg) {
    final int partition = messageId.getShard();
    final TopicPartition topicPartition = new TopicPartition(topic, partition);
    final long offset = messageId.getOffset();
    final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
    synchronized (consumerState.lock) {
      consumerState.offsetsConfirmed.put(topicPartition, offsetAndMetadata);
      consumerState.offsetsPending.computeIfPresent(partition, (_partition, _offset) -> {
        if (_offset == offset) {
          zlg.t("Confirmed last %d (partition %d)", z -> z.arg(_offset).arg(partition));
          return null;
        } else {
          zlg.t("Confirmed intermediate %d from %d (partition %d)", z -> z.arg(offset).arg(_offset).arg(partition));
          return _offset;
        }
      });
    }
  }

  private static void logException(Zlg zlg, Exception cause, String messageFormat, Object... messageArgs) {
    if (cause != null) {
      zlg.w(String.format(messageFormat, messageArgs), cause);
    }
  }

  @Override
  public void dispose() {
    Terminator.blank()
    .add(retryThread)
    .add(receivers)
    .add(consumerPipes)
    .add(producerPipe)
    .add(flows)
    .terminate()
    .joinSilently();
    CodecRegistry.deregister(codecLocator);
  }
}
