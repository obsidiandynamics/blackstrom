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
  private ProducerPipeConfig producerPipeConfig = new ProducerPipeConfig().withAsync(true);
  
  @YInject
  private ConsumerPipeConfig consumerPipeConfig = new ConsumerPipeConfig().withAsync(true).withBacklogBatches(16);
  
  @YInject
  private int maxConsumerPipeYields = 100;
  
  @YInject
  private Zlg zlg = Zlg.forClass(KafkaLedger.class).get();
  
  @YInject
  private int ioRetries = 10;
  
  @YInject
  private boolean drainConfirmations = true;
  
  /** Upper bound on the time to allow for offset drainage (in milliseconds). The default value 
   *  is based on the default of {@code max.poll.interval.ms} — {@code 300_000}. */
  @YInject
  private int drainConfirmationsTimeout = 300_000;
  
  @YInject
  private boolean printConfig;

  public Kafka<String, Message> getKafka() {
    return kafka;
  }

  public void setKafka(Kafka<String, Message> kafka) {
    this.kafka = kafka;
  }

  public KafkaLedgerConfig withKafka(Kafka<String, Message> kafka) {
    setKafka(kafka);
    return this;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public KafkaLedgerConfig withTopic(String topic) {
    setTopic(topic);
    return this;
  }

  public MessageCodec getCodec() {
    return codec;
  }

  public void setCodec(MessageCodec codec) {
    this.codec = codec;
  }

  public KafkaLedgerConfig withCodec(MessageCodec codec) {
    setCodec(codec);
    return this;
  }

  public ProducerPipeConfig getProducerPipeConfig() {
    return producerPipeConfig;
  }

  public void setProducerPipeConfig(ProducerPipeConfig producerPipeConfig) {
    this.producerPipeConfig = producerPipeConfig;
  }

  public KafkaLedgerConfig withProducerPipeConfig(ProducerPipeConfig producerPipeConfig) {
    setProducerPipeConfig(producerPipeConfig);
    return this;
  }

  public ConsumerPipeConfig getConsumerPipeConfig() {
    return consumerPipeConfig;
  }

  public void setConsumerPipeConfig(ConsumerPipeConfig consumerPipeConfig) {
    this.consumerPipeConfig = consumerPipeConfig;
  }

  public KafkaLedgerConfig withConsumerPipeConfig(ConsumerPipeConfig consumerPipeConfig) {
    setConsumerPipeConfig(consumerPipeConfig);
    return this;
  }
  
  public int getMaxConsumerPipeYields() {
    return maxConsumerPipeYields;
  }
  
  public void setMaxConsumerPipeYields(int maxConsumerPipeYields) {
    this.maxConsumerPipeYields = maxConsumerPipeYields;
  }
  
  public KafkaLedgerConfig withMaxConsumerPipeYields(int maxConsumerPipeYields) {
    setMaxConsumerPipeYields(maxConsumerPipeYields);
    return this;
  }

  public Zlg getZlg() {
    return zlg;
  }

  public void setZlg(Zlg zlg) {
    this.zlg = zlg;
  }

  public KafkaLedgerConfig withZlg(Zlg zlg) {
    setZlg(zlg);
    return this;
  }

  public int getIoRetries() {
    return ioRetries;
  }
  
  public void setIoRetries(int ioRetries) {
    this.ioRetries = ioRetries;
  }
  
  public KafkaLedgerConfig withIoRetries(int ioRetries) {
    setIoRetries(ioRetries);
    return this;
  }
  
  public boolean isDrainConfirmations() {
    return drainConfirmations;
  }
  
  public void setDrainConfirmations(boolean drainConfirmations) {
    this.drainConfirmations = drainConfirmations;
  }
  
  public KafkaLedgerConfig withDrainConfirmations(boolean drainConfirmations) {
    setDrainConfirmations(drainConfirmations);
    return this;
  }
  
  public boolean isPrintConfig() {
    return printConfig;
  }

  public void setPrintConfig(boolean printConfig) {
    this.printConfig = printConfig;
  }
  
  public KafkaLedgerConfig withPrintConfig(boolean printConfig) {
    setPrintConfig(printConfig);
    return this;
  }
  
  public int getDrainConfirmationsTimeout() {
    return drainConfirmationsTimeout;
  }
  
  public void setDrainConfirmationsTimeout(int drainConfirmationsTimeoutMillis) {
    this.drainConfirmationsTimeout = drainConfirmationsTimeoutMillis;
  }
  
  public KafkaLedgerConfig withDrainConfirmationsTimeout(int drainConfirmationsTimeoutMillis) {
    setDrainConfirmationsTimeout(drainConfirmationsTimeoutMillis);
    return this;
  }

  @Override
  public String toString() {
    return KafkaLedgerConfig.class.getSimpleName() + " [kafka=" + kafka + ", topic=" + topic + ", codec=" + codec + 
        ", producerPipeConfig=" + producerPipeConfig + ", consumerPipeConfig=" + consumerPipeConfig + 
        ", maxConsumerPipeYields=" + maxConsumerPipeYields + 
        ", ioRetries=" + ioRetries + ", drainConfirmations=" + drainConfirmations + 
        ", drainConfirmationsTimeout=" + drainConfirmationsTimeout + ", printConfig=" + printConfig + "]";
  }
}
