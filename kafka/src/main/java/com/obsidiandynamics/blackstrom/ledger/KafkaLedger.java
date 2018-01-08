package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.kafka.KafkaReceiver.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaLedger implements Ledger {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLedger.class);
  
  private static final long POLL_TIMEOUT_MILLIS = 1000;
  
  private final Kafka<String, Message> kafka;
  
  private final String topic;
  
  private final Producer<String, Message> producer;
  
  private final List<KafkaReceiver<String, Message>> receivers = new CopyOnWriteArrayList<>();
  
  private final Map<String, Consumer<String, Message>> consumers = new ConcurrentHashMap<>();
  
  private final MessageContext context = new DefaultMessageContext(this);
  
  public KafkaLedger(Kafka<String, Message> kafka, String topic) {
    this.kafka = kafka;
    this.topic = topic;
    
    final Properties props = new PropertiesBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", StringSerializer.class.getName())
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", Integer.MAX_VALUE)
        .build();
    producer = kafka.getProducer(props);
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
        .with("enable.auto.commit", String.valueOf(false))
        .with("session.timeout.ms", 6_000)
        .with("heartbeat.interval.ms", 2_000)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", KafkaJacksonMessageDeserializer.class.getName())
        .with(KafkaJacksonMessageDeserializer.CONFIG_MAP_PAYLOAD, String.valueOf(false))
        .build();
    final Consumer<String, Message> consumer = kafka.getConsumer(props);
    consumer.subscribe(Collections.singletonList(topic));
    
    final RecordHandler<String, Message> recordHandler = records -> {
      for (ConsumerRecord<String, Message> record : records) {
        final KafkaMessageId messageId = KafkaMessageId.fromRecord(record);
        final Message message = record.value();
        message.withMessageId(messageId);
        handler.onMessage(context, message);
      }
    };
    
    final String threadName = getClass().getSimpleName() + "-receiver-" + groupId;
    final KafkaReceiver<String, Message> receiver = new KafkaReceiver<>(consumer, POLL_TIMEOUT_MILLIS, 
        threadName, recordHandler, KafkaReceiver.genericErrorLogger(LOG));
    receivers.add(receiver);
    if (groupId != null) {
      consumers.put(groupId, consumer);
    }
  }

  @Override
  public void append(Message message) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void confirm(String groupId, Object messageId) {
    if (groupId != null) {
      final Consumer<?, ?> consumer = consumers.get(groupId);
      final KafkaMessageId kafkaMessageId = (KafkaMessageId) messageId;
      consumer.commitAsync(kafkaMessageId.toOffset(), (offsets, exception) -> {
        LOG.warn("Error committing " + messageId, exception);
      });
    }
  }

  @Override
  public void dispose() {
    receivers.forEach(t -> t.terminate());
    receivers.forEach(t -> t.joinQuietly());
  }
}
