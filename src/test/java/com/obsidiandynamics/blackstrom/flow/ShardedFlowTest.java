package com.obsidiandynamics.blackstrom.flow;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;

public class ShardedFlowTest {
  private final Timesert wait = Wait.SHORT;
  
  private ShardedFlow tracer;
  
  @Before
  public void before() {
    tracer = new ShardedFlow();
  }
  
  @After
  public void after() {
    if (tracer != null) tracer.dispose();
  }

  @Test
  public void testShards() {
    final List<Long> confirmed = new CopyOnWriteArrayList<>();
    final MessageContext context = new MessageContext() {
      @Override public Ledger getLedger() {
        throw new UnsupportedOperationException();
      }
      
      @Override public Object getHandlerId() {
        throw new UnsupportedOperationException();
      }
      
      @Override public void confirm(Object messageId) {
        confirmed.add(Cast.from(messageId));
      }
    };
    
    final Confirmation a0 = tracer.begin(context, message(0, 0));
    final Confirmation a1 = tracer.begin(context, message(1, 0));
    final Confirmation a2 = tracer.begin(context, message(2, 1));
    final Confirmation a3 = tracer.begin(context, message(3, 1));
    
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
    return new Proposal(ballotId, new String[0], null, 0).withShard(shard).withMessageId(ballotId);
  }
}
