package com.obsidiandynamics.blackstrom.tracer;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;

public class ShardedTracerTest {
  private final Timesert wait = Wait.SHORT;
  
  private ShardedTracer tracer;
  
  @Before
  public void before() {
    tracer = new ShardedTracer();
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
    
    final Action a0 = tracer.begin(context, message(0, 0));
    final Action a1 = tracer.begin(context, message(1, 0));
    final Action a2 = tracer.begin(context, message(2, 1));
    final Action a3 = tracer.begin(context, message(3, 1));
    
    a1.complete();
    a3.complete();
    assertEquals(0, confirmed.size());
    
    a2.complete();
    wait.until(() -> {
      assertEquals(Arrays.asList(3L), confirmed);
    });
    
    a0.complete();
    wait.until(() -> {
      assertEquals(Arrays.asList(3L, 1L), confirmed);
    });
  }

  private static Message message(long ballotId, int shard) {
    return new Nomination(ballotId, new String[0], null, 0).withShard(shard).withMessageId(ballotId);
  }
}
