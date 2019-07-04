package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import java.util.stream.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.blackstrom.spotter.*;
import com.obsidiandynamics.flow.Flow;
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
  private static final int POLL_TIMEOUT_MILLIS = 100;
  private static final int PIPELINE_BACKOFF_MILLIS = 1;
  private static final int RETRY_BACKOFF_MILLIS = 100;
  private static final long OFFSET_DRAIN_CHECK_INTERVAL_MILLIS = 10;

  private final Kafka<String, Message> kafka;
  
  private final String topic;

  private final Zlg zlg;

  /** A process-unique handle to the codec that's held by the {@link CodecRegistry}. */
  private final String codecLocator;
  
  /** Prevents deregistering the codec while new consumers are being attached to the ledger. */
  private final ReentrantReadWriteLock codecLock = new ReentrantReadWriteLock();

  private final ConsumerPipeConfig consumerPipeConfig;
  
  private final int maxConsumerPipeYields;
  
  private final SpotterConfig spotterConfig;

  private final Producer<String, Message> producer;

  private final ProducerPipe<String, Message> producerPipe;

  private final List<AsyncReceiver<String, Message>> receivers = new CopyOnWriteArrayList<>();

  private final List<ConsumerPipe<String, Message>> consumerPipes = new CopyOnWriteArrayList<>();
  
  private final List<ShardedFlow> flows = new CopyOnWriteArrayList<>();
  
  private final boolean printConfig;
  
  private final int ioRetries;
  
  private final boolean drainConfirmations;
  
  private final int drainConfirmationsTimeout;
  
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
    static final class MutableOffset {
      long offset = -1;

      boolean tryAdvance(long newOffset) {
        if (canAdvance(newOffset)) {
          offset = newOffset;
          return true;
        } else {
          return false;
        }
      }

      boolean canAdvance(long newOffset) {
        return newOffset > offset;
      }
    }
    
    final Object lock = new Object();
    
    /** The total number of records that have been queued through the consumer pipeline that are yet to
     *  be considered for processing. (Some of these records may be discarded when they emerge from the
     *  pipeline.) */
    int queuedRecords;
    
    /** Offsets that have been marked as confirmed (via the {@link Flow}) instances. */
    Map<TopicPartition, OffsetAndMetadata> offsetsConfirmed = new HashMap<>();
    
    /** Pending offsets for messages that have been accepted for processing, but haven't yet been confirmed. */
    final Map<Integer, Long> offsetsPending = new HashMap<>();
    
    /** Offsets for messages that have been accepted for processing. This map is cleared after drainage. */
    Map<Integer, Long> offsetsAccepted = new HashMap<>();
    
    /** A perpetual tracking of all offsets that have been processed, ensuring monotonicity. */
    final Map<Integer, MutableOffset> offsetsProcessed = new HashMap<>();
    
    /** Partitions that have been assigned to this consumer. */
    Set<Integer> assignedPartitions = Collections.emptySet();
    
    /** Partitions that are still held by this consumer. Differs from {@link #assignedPartitions} in that
     *  it doesn't get cleared in the {@code onPartitionsRevoked()} callback and persists until the
     *  {@code onPartitionsAssigned()} callback. */
    Set<Integer> heldPartitions = Collections.emptySet();
    
    MutableOffset processedOffsetForPartition(int partition) {
      return offsetsProcessed.computeIfAbsent(partition, __ -> new MutableOffset());
    }
  }

  /** Maps handler IDs to consumer offsets. */
  private final Map<Integer, ConsumerState> consumerStates = new ConcurrentHashMap<>();

  private final AtomicInteger nextHandlerId = new AtomicInteger();
  
  private volatile boolean disposing;

  public KafkaLedger(KafkaLedgerConfig config) {
    mustExist(config, "Config cannot be null");
    kafka = mustExist(config.getKafka(), "Kafka cannot be null");
    topic = mustExist(config.getTopic(), "Topic cannot be null");
    zlg = mustExist(config.getZlg(), "Zlg cannot be null");
    printConfig = mustExist(config.isPrintConfig(), "Print config cannot be null");
    consumerPipeConfig = mustExist(config.getConsumerPipeConfig(), "Consumer pipe config cannot be null");
    maxConsumerPipeYields = config.getMaxConsumerPipeYields();
    ioRetries = config.getIoRetries();
    drainConfirmations = config.isDrainConfirmations();
    drainConfirmationsTimeout = config.getDrainConfirmationsTimeout();
    spotterConfig = config.getSpotterConfig();
    final var codec = mustExist(config.getCodec(), "Codec cannot be null");
    codecLocator = CodecRegistry.register(codec);
    retryThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(KafkaLedger.class, "retry", topic))
        .onCycle(this::onRetry)
        .buildAndStart();

    // may be user-specified in config
    final var producerDefaults = new PropsBuilder()
        .withSystemDefault(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
        .withSystemDefault(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 18)
        .withSystemDefault(ProducerConfig.LINGER_MS_CONFIG, 1)
        .withSystemDefault(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
        .withSystemDefault(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120_000)
        .withSystemDefault(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_001)
        .withSystemDefault(ProducerConfig.RETRIES_CONFIG, 10_000)
        .withSystemDefault(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100)
        .build();

    // set by the application — required for correctness (overrides user config)
    final var producerOverrides = new PropsBuilder()
        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaMessageSerializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .with(ProducerConfig.ACKS_CONFIG, "all")
        .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
        .with(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3600 * 1000)
        .build();
    
    if (printConfig) kafka.describeProducer(zlg::i, producerDefaults, producerOverrides);
    producer = kafka.getProducer(producerDefaults, producerOverrides);
    final var producerPipeThreadName = ProducerPipe.class.getSimpleName() + "-" + topic;
    final var producerPipeConfig = mustExist(config.getProducerPipeConfig(), "Producer pipe config");
    producerPipe = new ProducerPipe<>(producerPipeConfig, producer, producerPipeThreadName, zlg::w);
  }
  
  private void onRetry(WorkerThread t) throws InterruptedException {
    final var retryTask = retryQueueConsumer.poll();
    if (retryTask != null) {
      append(retryTask.message, retryTask.callback);
    } else {
      Thread.sleep(RETRY_BACKOFF_MILLIS);
    }
  }
  
  private static Map<TopicPartition, OffsetAndMetadata> toKafkaOffsetsMap(Map<Integer, Long> sourceMap, String topic) {
    final var targetMap = new HashMap<TopicPartition, OffsetAndMetadata>();
    for (var entry : sourceMap.entrySet()) {
      targetMap.put(new TopicPartition(topic, entry.getKey()), new OffsetAndMetadata(entry.getValue()));
    }
    return targetMap;
  }

  @Override
  public Object attach(MessageHandler handler) {
    mustExist(handler, "Handler cannot be null");
    codecLock.readLock().lock();
    try {
      if (! disposing) {
        return _attach(handler);
      } else {
        return null;
      }
    } finally {
      codecLock.readLock().unlock();
    }
  }

  private Integer _attach(MessageHandler handler) {
    final var groupId = handler.getGroupId();
    final String autoOffsetReset;
    if (groupId != null) {
      autoOffsetReset = OffsetResetStrategy.EARLIEST.name().toLowerCase();
    } else {
      autoOffsetReset = OffsetResetStrategy.LATEST.name().toLowerCase();
    }
    
    // may be user-specified in config
    final var consumerDefaults = new PropsBuilder()
        .withSystemDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6_000)
        .withSystemDefault(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2_000)
        .withSystemDefault(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10_000)
        .build();

    // set by the application — required for correctness (overrides user config)
    final var consumerOverrides = new PropsBuilder()
        .with(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
        .with(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        .with(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
        .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaMessageDeserializer.class.getName())
        .with(CodecRegistry.CONFIG_CODEC_LOCATOR, codecLocator)
        .build();
    
    if (printConfig) kafka.describeConsumer(zlg::i, consumerDefaults, consumerOverrides);
    final var consumer = kafka.getConsumer(consumerDefaults, consumerOverrides);
    final ConsumerState consumerState;
    if (groupId != null) {
      consumerState = new ConsumerState();
    } else {
      consumerState = null;
    }
    
    final var spotter = new Spotter(spotterConfig);
    
    new Retry()
    .withAttempts(ioRetries)
    .withFaultHandler(zlg::w)
    .withErrorHandler(zlg::e)
    .withExceptionMatcher(Retry.isA(RetriableException.class))
    .run(() -> {
      if (groupId != null) {
        final var rebalanceListener = new ConsumerRebalanceListener() {
          final Predicate<TopicPartition> topicFilter = fieldPredicate(TopicPartition::topic, topic::equals);
          
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            synchronized (consumerState.lock) {
              consumerState.assignedPartitions = Collections.emptySet();
            }

            if (drainConfirmations) {
              // blocks until all pending offsets have been confirmed, and performs a sync commit
              drainOffsets(topic, consumer, consumerState, ioRetries, sleepFor(OFFSET_DRAIN_CHECK_INTERVAL_MILLIS), 
                           drainConfirmationsTimeout, KafkaLedger.this::isDisposing, zlg);
            }
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            final var assignedPartitions = partitions.stream()
                .filter(topicFilter)
                .collect(Collectors.toSet());
            
            final var assignedPartitionIndexes = assignedPartitions.stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());
            
            synchronized (consumerState.lock) {
              consumerState.assignedPartitions = assignedPartitionIndexes;
              consumerState.heldPartitions = assignedPartitionIndexes;
            }
            
            // add/remove lots from the spotter in accordance with our newly assigned partitions
            final var lots = spotter.getLots();
            for (var shard : lots.keySet()) {
              if (! assignedPartitionIndexes.contains(shard)) {
                spotter.removeLot(shard);
              }
            }
            for (var shard : assignedPartitionIndexes) {
              spotter.addLot(shard);
            }
          }
        };
        consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
        zlg.d("Subscribed to topic %s", z -> z.arg(topic));
      } else {
        final var infos = consumer.partitionsFor(topic);
        final var partitions = infos.stream()
            .map(i -> new TopicPartition(i.topic(), i.partition()))
            .collect(Collectors.toList());
        final var endOffsets = consumer.endOffsets(partitions);
        zlg.t("Assigning all partitions; end offsets: %s", z -> z.arg(endOffsets));
        consumer.assign(partitions);
        for (var entry : endOffsets.entrySet()) {
          consumer.seek(entry.getKey(), entry.getValue());
        }
      }
    });

    final Integer handlerId;
    final Retention retention;
    if (groupId != null) {
      handlerId = nextHandlerId.getAndIncrement();
      consumerStates.put(handlerId, consumerState);
      final var flow = new ShardedFlow(groupId);
      retention = flow;
      flows.add(flow);
    } else {
      handlerId = null;
      retention = NopRetention.getInstance();
    }

    final var context = new DefaultMessageContext(this, handlerId, retention);
    final var consumerPipeThreadName = ConsumerPipe.class.getSimpleName() + "-" + groupId;
    final RecordHandler<String, Message> pipelinedRecordHandler = records -> {
      for (var record : records) {
        handleRecord(handler, consumerState, context, record, zlg);
      }
    };
    final var consumerPipe = 
        new ConsumerPipe<>(consumerPipeConfig, pipelinedRecordHandler, consumerPipeThreadName);
    consumerPipes.add(consumerPipe);
    final RecordHandler<String, Message> recordHandler = records -> {
      for (var record : records) {
        spotter.tryAdvance(record.partition(), record.offset());
      }
      spotter.printParkedLots();
      queueRecords(consumer, consumerState, consumerPipe, records, maxConsumerPipeYields, zlg);
    };

    final var threadName = KafkaLedger.class.getSimpleName() + "-receiver-" + groupId;
    final var receiver = new AsyncReceiver<>(consumer, POLL_TIMEOUT_MILLIS, 
        threadName, recordHandler, zlg::w);
    receivers.add(receiver);
    
    return handlerId;
  }
  
  static void queueRecords(Consumer<String, Message> consumer, 
                           ConsumerState consumerState,
                           ConsumerPipe<String, Message> consumerPipe,
                           ConsumerRecords<String, Message> records,
                           int maxConsumerPipeYields,
                           Zlg zlg) throws InterruptedException {
    final boolean queueBatch;
    if (consumerState != null) {
      final var recordCount = records.count();
      synchronized (consumerState.lock) {
        if (! consumerState.assignedPartitions.isEmpty()) {
          consumerState.queuedRecords += recordCount;
          queueBatch = true;
        } else {
          queueBatch = false;
        }
      }
    } else {
      queueBatch = true;
    }
    
    if (queueBatch) {
      for (int yields = 0;;) {
        final var enqueued = consumerPipe.receive(records);
 
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
    }
  }
  
  static Runnable sleepFor(long sleepMillis) {
    return () -> Threads.sleep(sleepMillis);
  }
  
  boolean isDisposing() {
    return disposing;
  }

  static void drainOffsets(String topic, Consumer<String, Message> consumer, ConsumerState consumerState, 
                           int ioRetries, Runnable intervalSleep, int drainTimeoutMillis, 
                           BooleanSupplier isDisposingCheck, Zlg zlg) {
    final long drainUntilTime = System.currentTimeMillis() + drainTimeoutMillis;
    while (! isDisposingCheck.getAsBoolean()) {
      final boolean allPendingOffsetsConfirmed;
      final Map<Integer, Long> offsetsAcceptedSnapshot;
      synchronized (consumerState.lock) {
        if (consumerState.queuedRecords == 0) {
          allPendingOffsetsConfirmed = consumerState.offsetsPending.isEmpty();
          if (allPendingOffsetsConfirmed) {
            offsetsAcceptedSnapshot = consumerState.offsetsAccepted;
            consumerState.offsetsAccepted = new HashMap<>();
          } else {
            zlg.d("Offsets pending: %s", z -> z.arg(consumerState.offsetsPending));
            offsetsAcceptedSnapshot = null;
          }
        } else {
          zlg.d("Pipeline backlogged: %d records queued", z -> z.arg(consumerState.queuedRecords));
          allPendingOffsetsConfirmed = false;
          offsetsAcceptedSnapshot = null;
        }
      }

      if (allPendingOffsetsConfirmed) {
        zlg.d("All offsets confirmed: %s", z -> z.arg(offsetsAcceptedSnapshot));
        if (! offsetsAcceptedSnapshot.isEmpty()) {
          final var kafkaOffsets = toKafkaOffsetsMap(offsetsAcceptedSnapshot, topic);
          new Retry()
          .withAttempts(ioRetries)
          .withFaultHandler(zlg::w)
          .withErrorHandler(zlg::e)
          .withExceptionMatcher(Retry.isA(RetriableException.class))
          .run(() -> consumer.commitSync(kafkaOffsets));
        }
        return;
      } else if (System.currentTimeMillis() < drainUntilTime) {
        intervalSleep.run();
      } else {
        synchronized (consumerState.lock) {
          zlg.w("Drain timed out (%,d ms). Pending offsets %s, %d records queued", 
                z -> z.arg(drainTimeoutMillis).arg(consumerState.offsetsPending).arg(consumerState.queuedRecords));
        }
        return;
      }
    }
  }

  static void commitOffsets(Consumer<String, Message> consumer, ConsumerState consumerState, Zlg zlg) {
    final Map<TopicPartition, OffsetAndMetadata> confirmedSnapshot;
    synchronized (consumerState.lock) {
      if (! consumerState.offsetsConfirmed.isEmpty()) {
        confirmedSnapshot = consumerState.offsetsConfirmed;
        consumerState.offsetsConfirmed = new HashMap<>(confirmedSnapshot.size());
        
        for (var partitionOffset : confirmedSnapshot.entrySet()) {
          final var partition = partitionOffset.getKey().partition();
          // the committed offset is always the last processed offset + 1
          final var offset = partitionOffset.getValue().offset();
          consumerState.processedOffsetForPartition(partition).tryAdvance(offset - 1);
        }
      } else {
        confirmedSnapshot = null;
      }
    }

    if (confirmedSnapshot != null) {
      zlg.t("Committing offsets %s", z -> z.arg(confirmedSnapshot));
      consumer.commitAsync(confirmedSnapshot, 
                           (offsets, exception) -> maybeLogException(zlg, exception, "Error committing offsets %s", offsets));
    }
  }

  static void handleRecord(MessageHandler handler, ConsumerState consumerState, MessageContext context,
                           ConsumerRecord<String, Message> record, Zlg zlg) {
    final var partition = record.partition();
    final var offset = record.offset();
    final boolean accepted;
    if (consumerState != null) {
      synchronized (consumerState.lock) {
        consumerState.queuedRecords--;
        final var canAccept = consumerState.assignedPartitions.contains(partition);
        if (canAccept) {
          final var mutableOffset = consumerState.processedOffsetForPartition(partition);
          accepted = mutableOffset.canAdvance(offset);
          if (accepted) {
            consumerState.offsetsPending.put(partition, offset);
            consumerState.offsetsAccepted.put(partition, offset);
          } else {
            zlg.d("Skipping message at offset %d, partition %d: monotonicity constraint breached (previous offset %d)", 
                  z -> z.arg(record::offset).arg(record::partition).arg(mutableOffset.offset));
          }
        } else {
          accepted = false;
          zlg.d("Skipping message at offset %d: partition %d has been reassigned", z -> z.arg(record::offset).arg(record::partition));
        }
      }
    } else {
      accepted = true;
    }
    
    if (accepted) {
      final var messageId = new DefaultMessageId(partition, offset);
      final var message = record.value();
      message.setMessageId(messageId);
      message.setShardKey(record.key());
      message.setShard(partition);
      handler.onMessage(context, message);
    }
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    mustExist(message, "Message cannot be null");
    mustExist(callback, "Callback cannot be null");
    final var record = 
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
    mustExist(handlerId, illegalState("Cannot confirm in an ungrouped context"));
    mustExist(messageId, "Message ID cannot be null");
    final var consumerState = consumerStates.get(handlerId);
    confirm((DefaultMessageId) messageId, consumerState, topic, zlg);
  }

  static void confirm(DefaultMessageId messageId, ConsumerState consumerState, String topic, Zlg zlg) {
    final var partition = messageId.getShard();
    final var topicPartition = new TopicPartition(topic, partition);
    final var offset = messageId.getOffset();
    final var offsetAndMetadata = new OffsetAndMetadata(offset + 1); // committed offset is last processed + 1
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

  private static void maybeLogException(Zlg zlg, Exception exception, String messageFormat, Object messageArg) {
    if (exception != null) logException(zlg, exception, messageFormat, messageArg);
  }

  private static void logException(Zlg zlg, Exception exception, String messageFormat, Object messageArg) {
    zlg.w(messageFormat, z -> z.arg(messageArg).threw(exception));
  }

  @Override
  public boolean isAssigned(Object handlerId, int shard) {
    if (handlerId == null) {
      return true;
    } else {
      final var consumerState = consumerStates.get(handlerId);
      synchronized (consumerState.lock) {
        return consumerState.heldPartitions.contains(shard);
      }
    }
  }

  @Override
  public void dispose() {
    disposing = true;
    Terminator.blank()
    .add(retryThread)
    .add(receivers)
    .add(consumerPipes)
    .add(producerPipe)
    .add(flows)
    .terminate()
    .joinSilently();
    codecLock.writeLock().lock();
    try {
      CodecRegistry.deregister(codecLocator);
    } finally {
      codecLock.writeLock().unlock();
    }
  }
}
