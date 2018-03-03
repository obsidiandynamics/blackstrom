package com.obsidiandynamics.blackstrom.retention;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;

public class ShardedFlowTest {
  private final Timesert wait = Wait.SHORT;
  
  private ShardedFlow flow;
  
  @Before
  public void before() {
    flow = new ShardedFlow();
  }
  
  @After
  public void after() {
    if (flow != null) flow.dispose();
  }

  @Test
  public void testShards() {
    final List<Long> confirmed = new CopyOnWriteArrayList<>();
    final Ledger ledger = new Ledger() {
      @Override public void attach(MessageHandler handler) {
        throw new UnsupportedOperationException();
      }

      @Override public void append(Message message, AppendCallback callback) {
        throw new UnsupportedOperationException();
      }

      @Override public void confirm(Object handlerId, MessageId messageId) {
        confirmed.add(((DefaultMessageId) messageId).getOffset());
      }
    };
    final MessageContext context = new MessageContext() {
      @Override public Ledger getLedger() {
        return ledger;
      }
      
      @Override public Object getHandlerId() {
        return null;
      }
      
      @Override public void confirm(Message message) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Retention getRetention() {
        return flow;
      }
    };
    
    final Confirmation a0 = flow.begin(context, message(0, 0));
    final Confirmation a1 = flow.begin(context, message(1, 0));
    final Confirmation a2 = flow.begin(context, message(2, 1));
    final Confirmation a3 = flow.begin(context, message(3, 1));
    
    a1.confirm();
    a3.confirm();
    assertEquals(0, confirmed.size());
    
    a2.confirm();
    wait.until(() -> {
      assertEquals(Arrays.asList(3L), confirmed);
    });
    
    a0.confirm();
    wait.until(() -> {
      assertEquals(Arrays.asList(3L, 1L), confirmed);
    });
  }

  private static Message message(long ballotId, int shard) {
    return new Proposal(String.valueOf(ballotId), new String[0], null, 0)
        .withShard(shard)
        .withMessageId(new DefaultMessageId(shard, ballotId));
  }
}
