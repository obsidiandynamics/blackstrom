package com.obsidiandynamics.blackstrom.ledger;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class HazelQLedgerConfig {
  @YInject
  private String stream; 
  
  @YInject
  private MessageCodec codec;
  
  @YInject
  private Logger log = LoggerFactory.getLogger(HazelQLedger.class);
  
  @YInject
  private StreamConfig streamConfig = new StreamConfig();

  String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  MessageCodec getCodec() {
    return codec;
  }

  public void setCodec(MessageCodec codec) {
    this.codec = codec;
  }

  Logger getLog() {
    return log;
  }

  public void setLog(Logger log) {
    this.log = log;
  }

  StreamConfig getStreamConfig() {
    return streamConfig;
  }

  public void setStreamConfig(StreamConfig streamConfig) {
    this.streamConfig = streamConfig;
  }
}
