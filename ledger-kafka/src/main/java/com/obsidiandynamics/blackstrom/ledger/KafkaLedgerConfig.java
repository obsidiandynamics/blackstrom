package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.zerolog.*;

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
  private int maxConsumerPipeYields = 100;
  
  @YInject
  private Zlg zlg = Zlg.forDeclaringClass().get();
  
  @YInject
  private int ioRetries = 10;
  
  @YInject
  private boolean drainConfirmations;
  
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
  
  int getMaxConsumerPipeYields() {
    return maxConsumerPipeYields;
  }
  
  public KafkaLedgerConfig withMaxConsumerPipeYields(int maxConsumerPipeYields) {
    this.maxConsumerPipeYields = maxConsumerPipeYields;
    return this;
  }

  Zlg getZlg() {
    return zlg;
  }

  public KafkaLedgerConfig withZlg(Zlg zlg) {
    this.zlg = zlg;
    return this;
  }

  int getIORetries() {
    return ioRetries;
  }
  
  public KafkaLedgerConfig withIORetries(int ioRetries) {
    this.ioRetries = ioRetries;
    return this;
  }
  
  boolean isDrainConfirmations() {
    return drainConfirmations;
  }
  
  public KafkaLedgerConfig withDrainConfirmations(boolean drainConfirmations) {
    this.drainConfirmations = drainConfirmations;
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
        ", producerPipeConfig=" + producerPipeConfig + ", consumerPipeConfig=" + consumerPipeConfig + 
        ", maxConsumerPipeYields=" + maxConsumerPipeYields + 
        ", ioRetries=" + ioRetries + ", drainConfirmations=" + drainConfirmations + 
        ", printConfig=" + printConfig + "]";
  }
}
