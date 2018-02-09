package com.obsidiandynamics.blackstrom.ledger;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaLedgerOptions {
  private Kafka<String, Message> kafka;
  
  private String topic; 
  
  private MessageCodec codec;
  
  private ProducerPipeOptions producerPipeOptions = new ProducerPipeOptions();
  
  private ConsumerPipeOptions consumerPipeOptions = new ConsumerPipeOptions();
  
  private Logger log = LoggerFactory.getLogger(KafkaLedger.class);

  Kafka<String, Message> getKafka() {
    return kafka;
  }

  public KafkaLedgerOptions withKafka(Kafka<String, Message> kafka) {
    this.kafka = kafka;
    return this;
  }

  String getTopic() {
    return topic;
  }

  public KafkaLedgerOptions withTopic(String topic) {
    this.topic = topic;
    return this;
  }

  MessageCodec getCodec() {
    return codec;
  }

  public KafkaLedgerOptions withCodec(MessageCodec codec) {
    this.codec = codec;
    return this;
  }

  ProducerPipeOptions getProducerPipeOptions() {
    return producerPipeOptions;
  }

  public KafkaLedgerOptions withProducerPipeOptions(ProducerPipeOptions producerPipeOptions) {
    this.producerPipeOptions = producerPipeOptions;
    return this;
  }

  ConsumerPipeOptions getConsumerPipeOptions() {
    return consumerPipeOptions;
  }

  public KafkaLedgerOptions withConsumerPipeOptions(ConsumerPipeOptions consumerPipeOptions) {
    this.consumerPipeOptions = consumerPipeOptions;
    return this;
  }

  Logger getLog() {
    return log;
  }

  public KafkaLedgerOptions withLog(Logger log) {
    this.log = log;
    return this;
  }
  
  //TODO add toString
}
