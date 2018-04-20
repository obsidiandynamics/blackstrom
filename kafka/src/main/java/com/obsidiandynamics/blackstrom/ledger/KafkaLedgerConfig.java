package com.obsidiandynamics.blackstrom.ledger;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class KafkaLedgerConfig {
  @YInject
  private Kafka<String, Message> kafka;
  
  @YInject
  private String topic; 
  
  @YInject
  private MessageCodec codec;
  
  @YInject
  private ProducerPipeConfig producerPipeConfig = new ProducerPipeConfig().withAsync(false);
  
  @YInject
  private ConsumerPipeConfig consumerPipeConfig = new ConsumerPipeConfig().withAsync(true);
  
  @YInject
  private Logger log = LoggerFactory.getLogger(KafkaLedger.class);
  
  @YInject
  private int attachRetries = 10;
  
  @YInject
  private boolean printConfig;

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

  int getAttachRetries() {
    return attachRetries;
  }
  
  public KafkaLedgerConfig withAttachRetries(int attachRetries) {
    this.attachRetries = attachRetries;
    return this;
  }
  
  public KafkaLedgerConfig withPrintConfig(boolean printConfig) {
    this.printConfig = printConfig;
    return this;
  }
  
  boolean isPrintConfig() {
    return printConfig;
  }

  @Override
  public String toString() {
    return KafkaLedgerConfig.class.getSimpleName() + " [kafka=" + kafka + ", topic=" + topic + ", codec=" + codec + 
        ", producerPipeConfig=" + producerPipeConfig + ", consumerPipeConfig=" + consumerPipeConfig + ", log=" + log + 
        ", attachRetries=" + attachRetries + ", printConfig=" + printConfig + "]";
  }
}
