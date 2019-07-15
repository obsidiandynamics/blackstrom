package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.spotter.*;
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
  private int pollTimeout = 100;
  
  @YInject
  private int maxConsumerPipeYields = 100;
  
  @YInject
  private Zlg zlg = Zlg.forClass(KafkaLedger.class).get();
  
  @YInject
  private int ioAttempts = 10;
  
  @YInject
  private boolean drainConfirmations = true;
  
  @YInject
  private boolean enforceMonotonicity;
  
  /** Upper bound on the time to allow for offset drainage (in milliseconds). The default value 
   *  is based on the default of {@code max.poll.interval.ms} — {@code 300_000}. */
  @YInject
  private int drainConfirmationsTimeout = 300_000;
  
  @YInject
  private boolean printConfig;
  
  @YInject
  private SpotterConfig spotterConfig = new SpotterConfig();
  
  public void validate() {
    mustExist(kafka, "Kafka cannot be null");
    mustExist(topic, "Topic cannot be null");
    mustExist(codec, "Codec cannot be null");
    mustExist(producerPipeConfig, "Producer pipe config cannot be null");
    mustExist(consumerPipeConfig, "Consumer pipe config cannot be null");
    mustBeGreater(pollTimeout, 0, illegalArgument("Poll timeout must be greater than zero"));
    mustBeGreaterOrEqual(maxConsumerPipeYields, 0, illegalArgument("Max consumer pipe yields cannot be negative"));
    mustExist(zlg, "Zlg cannot be null");
    mustBeGreater(ioAttempts, 0, illegalArgument("IO attempts must be greater than zero"));
    mustBeGreaterOrEqual(drainConfirmationsTimeout, 0, illegalArgument("Drain confirmations timeout cannot be negative"));
    mustExist(spotterConfig, "Spotter config cannot be null").validate();
  }

  public Kafka<String, Message> getKafka() {
    return kafka;
  }

  public void setKafka(Kafka<String, Message> kafka) {
    this.kafka = mustExist(kafka, "Kafka cannot be null");
  }

  public KafkaLedgerConfig withKafka(Kafka<String, Message> kafka) {
    setKafka(kafka);
    return this;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = mustExist(topic, "Topic cannot be null");
  }

  public KafkaLedgerConfig withTopic(String topic) {
    setTopic(topic);
    return this;
  }

  public MessageCodec getCodec() {
    return codec;
  }

  public void setCodec(MessageCodec codec) {
    this.codec = mustExist(codec, "Codec cannot be null");
  }

  public KafkaLedgerConfig withCodec(MessageCodec codec) {
    setCodec(codec);
    return this;
  }

  public ProducerPipeConfig getProducerPipeConfig() {
    return producerPipeConfig;
  }

  public void setProducerPipeConfig(ProducerPipeConfig producerPipeConfig) {
    this.producerPipeConfig = mustExist(producerPipeConfig, "Producer pipe config cannot be null");
  }

  public KafkaLedgerConfig withProducerPipeConfig(ProducerPipeConfig producerPipeConfig) {
    setProducerPipeConfig(producerPipeConfig);
    return this;
  }

  public ConsumerPipeConfig getConsumerPipeConfig() {
    return consumerPipeConfig;
  }

  public void setConsumerPipeConfig(ConsumerPipeConfig consumerPipeConfig) {
    this.consumerPipeConfig = mustExist(consumerPipeConfig, "Consumer pipe config cannot be null");
  }

  public KafkaLedgerConfig withConsumerPipeConfig(ConsumerPipeConfig consumerPipeConfig) {
    setConsumerPipeConfig(consumerPipeConfig);
    return this;
  }
  
  public int getPollTimeout() {
    return pollTimeout;
  }
  
  public void setPollTimeout(int pollTimeoutMillis) {
    this.pollTimeout = mustBeGreater(pollTimeoutMillis, 0, illegalArgument("Poll timeout must be greater than zero"));
  }
  
  public KafkaLedgerConfig withPollTimeout(int pollTimeoutMillis) {
    setPollTimeout(pollTimeoutMillis);
    return this;
  }
  
  public int getMaxConsumerPipeYields() {
    return maxConsumerPipeYields;
  }
  
  public void setMaxConsumerPipeYields(int maxConsumerPipeYields) {
    this.maxConsumerPipeYields = mustBeGreaterOrEqual(maxConsumerPipeYields, 0, illegalArgument("Max consumer pipe yields cannot be negative"));
  }
  
  public KafkaLedgerConfig withMaxConsumerPipeYields(int maxConsumerPipeYields) {
    setMaxConsumerPipeYields(maxConsumerPipeYields);
    return this;
  }

  public Zlg getZlg() {
    return zlg;
  }

  public void setZlg(Zlg zlg) {
    this.zlg = mustExist(zlg, "Zlg cannot be null");
  }

  public KafkaLedgerConfig withZlg(Zlg zlg) {
    setZlg(zlg);
    return this;
  }

  public int getIoAttempts() {
    return ioAttempts;
  }
  
  public void setIoAttempts(int ioAttempts) {
    this.ioAttempts = mustBeGreater(ioAttempts, 0, illegalArgument("IO attempts must be greater than zero"));
  }
  
  public KafkaLedgerConfig withIoAttempts(int ioAttempts) {
    setIoAttempts(ioAttempts);
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
  
  public boolean isEnforceMonotonicity() {
    return enforceMonotonicity;
  }
  
  public void setEnforceMonotonicity(boolean enforceMonotonicity) {
    this.enforceMonotonicity = enforceMonotonicity;
  }
  
  public KafkaLedgerConfig withEnforceMonotonicity(boolean enforceMonotonicity) {
    setEnforceMonotonicity(enforceMonotonicity);
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
    this.drainConfirmationsTimeout = mustBeGreaterOrEqual(drainConfirmationsTimeoutMillis, 0, 
                                                          illegalArgument("Drain confirmations timeout cannot be negative"));
  }
  
  public KafkaLedgerConfig withDrainConfirmationsTimeout(int drainConfirmationsTimeoutMillis) {
    setDrainConfirmationsTimeout(drainConfirmationsTimeoutMillis);
    return this;
  }
  
  public SpotterConfig getSpotterConfig() {
    return spotterConfig;
  }

  public void setSpotterConfig(SpotterConfig spotterConfig) {
    this.spotterConfig = mustExist(spotterConfig, "Spotter config cannot be null");
  }

  public KafkaLedgerConfig withSpotterConfig(SpotterConfig spotterConfig) {
    setSpotterConfig(spotterConfig);
    return this;
  }

  @Override
  public String toString() {
    return KafkaLedgerConfig.class.getSimpleName() + " [kafka=" + kafka + ", topic=" + topic + ", codec=" + codec + 
        ", producerPipeConfig=" + producerPipeConfig + ", pollTimeout=" + pollTimeout + ", consumerPipeConfig=" + consumerPipeConfig + 
        ", maxConsumerPipeYields=" + maxConsumerPipeYields + 
        ", ioRetries=" + ioAttempts + ", drainConfirmations=" + drainConfirmations + ", enforceMonotonicity=" + enforceMonotonicity +
        ", drainConfirmationsTimeout=" + drainConfirmationsTimeout + ", printConfig=" + printConfig + 
        ", spotterConfig=" + spotterConfig + "]";
  }
}
