package com.obsidiandynamics.blackstrom.ledger;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class HazelQLedgerConfig {
  @YInject
  private MessageCodec codec;
  
  @YInject
  private Logger log = LoggerFactory.getLogger(HazelQLedger.class);
  
  @YInject
  private StreamConfig streamConfig = new StreamConfig();
  
  @YInject
  private ElectionConfig electionConfig = new ElectionConfig();
  
  @YInject
  private int pollIntervalMillis = 100;

  MessageCodec getCodec() {
    return codec;
  }

  public HazelQLedgerConfig withCodec(MessageCodec codec) {
    this.codec = codec;
    return this;
  }

  Logger getLog() {
    return log;
  }

  public HazelQLedgerConfig withLog(Logger log) {
    this.log = log;
    return this;
  }

  StreamConfig getStreamConfig() {
    return streamConfig;
  }

  public HazelQLedgerConfig withStreamConfig(StreamConfig streamConfig) {
    this.streamConfig = streamConfig;
    return this;
  }
  
  ElectionConfig getElectionConfig() {
    return electionConfig;
  }
  
  public HazelQLedgerConfig withElectionConfig(ElectionConfig electionConfig) {
    this.electionConfig = electionConfig;
    return this;
  }

  int getPollInterval() {
    return pollIntervalMillis;
  }

  public HazelQLedgerConfig withPollInterval(int pollIntervalMillis) {
    this.pollIntervalMillis = pollIntervalMillis;
    return this;
  }
}
