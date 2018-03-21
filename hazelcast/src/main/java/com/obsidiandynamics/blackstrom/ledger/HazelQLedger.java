package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import org.slf4j.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.model.Message;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class HazelQLedger implements Ledger {
  private final Logger log;
  
  private final StreamConfig streamConfig;
  
  private final Publisher publisher;
  
  private final MessageCodec codec;
  
//  private final List<ConsumerPipe<String, Message>> consumerPipes = new CopyOnWriteArrayList<>();
  
  private final List<ShardedFlow> flows = new CopyOnWriteArrayList<>(); 
  
  public HazelQLedger(HazelcastInstance instance, HazelQLedgerConfig config) {
    log = config.getLog();
    streamConfig = config.getStreamConfig();
    codec = config.getCodec();
    final PublisherConfig pubConfig = new PublisherConfig()
        .withLog(log)
        .withStreamConfig(streamConfig);
    publisher = Publisher.createDefault(instance, pubConfig);
  }
  
  private void onRecord(Record record) {
    //TODO
  }

  @Override
  public void attach(MessageHandler handler) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    try {
      final byte[] bytes = codec.encode(message);
      publisher.publishAsync(new Record(bytes), (offset, error) -> {
        final MessageId messageId = offset != Record.UNASSIGNED_OFFSET ? new DefaultMessageId(0, offset) : null;
        callback.onAppend(messageId, error);
      });
    } catch (Exception e) {
      callback.onAppend(null, e);
    }
    
  }

  @Override
  public void dispose() {
    final List<Terminable> terminables = new ArrayList<>();
    terminables.add(publisher);
    //TODO add the subs and receivers
    terminables.addAll(flows);
    final List<Joinable> joinables = terminables.stream().map(t -> t.terminate()).collect(Collectors.toList());
    joinables.forEach(j -> j.joinQuietly());
  }
}
