package com.obsidiandynamics.blackstrom.ledger;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaLedgerConfig {
  private Kafka<String, Message> kafka;
  
  private String topic; 
  
  private MessageCodec codec;
  
  private ProducerPipeConfig producerPipeConfig = new ProducerPipeConfig();
  
  private ConsumerPipeConfig consumerPipeConfig = new ConsumerPipeConfig();
  
  private Logger log = LoggerFactory.getLogger(KafkaLedger.class);

  Kafka<String, Message> getKafka() {
    return kafka;
  }

  public KafkaLedgerConfig withKafka(Kafka<String, Message> kafka) {
    this.kafka = kafka;
    return this;
  }

  String getTopic() {
    return topic;
  }

  public KafkaLedgerConfig withTopic(String topic) {
    this.topic = topic;
    return this;
  }

  MessageCodec getCodec() {
    return codec;
  }

  public KafkaLedgerConfig withCodec(MessageCodec codec) {
    this.codec = codec;
    return this;
  }

  ProducerPipeConfig getProducerPipeConfig() {
    return producerPipeConfig;
  }

  public KafkaLedgerConfig withProducerPipeConfig(ProducerPipeConfig producerPipeConfig) {
    this.producerPipeConfig = producerPipeConfig;
    return this;
  }

  ConsumerPipeConfig getConsumerPipeConfig() {
    return consumerPipeConfig;
  }

  public KafkaLedgerConfig withConsumerPipeConfig(ConsumerPipeConfig consumerPipeConfig) {
    this.consumerPipeConfig = consumerPipeConfig;
    return this;
  }

  Logger getLog() {
    return log;
  }

  public KafkaLedgerConfig withLog(Logger log) {
    this.log = log;
    return this;
  }

  @Override
  public String toString() {
    return KafkaLedgerConfig.class.getSimpleName() + " [kafka=" + kafka + ", topic=" + topic + ", codec=" + codec + 
        ", producerPipeConfig=" + producerPipeConfig + ", consumerPipeConfig=" + consumerPipeConfig + ", log=" + log + "]";
  }
}
