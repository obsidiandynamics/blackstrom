package com.obsidiandynamics.blackstrom.retention;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.flow.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.threads.*;

@RunWith(Parameterized.class)
public class ShardedFlowTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final Timesert wait = Wait.SHORT;
  
  private ShardedFlow flow;
  
  @Before
  public void before() {
    flow = new ShardedFlow();
  }
  
  @After
  public void after() {
    if (flow != null) {
      flow.terminate().joinSilently();
    }
  }

  /**
   *  Creates confirmations, confirms them, and verifies that confirmations
   *  have propagated back to the ledger.
   */
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
      
      @Override public void beginAndConfirm(Message message) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Retention getRetention() {
        return flow;
      }
    };
    
    final Confirmation c0 = flow.begin(context, message(0, 0));
    final Confirmation c1 = flow.begin(context, message(1, 0));
    final Confirmation c2 = flow.begin(context, message(2, 1));
    final Confirmation c3 = flow.begin(context, message(3, 1));
    
    c1.confirm();
    c3.confirm();
    assertEquals(0, confirmed.size());
    
    c2.confirm();
    wait.until(() -> {
      assertEquals(Arrays.asList(3L), confirmed);
    });
    
    c0.confirm();
    wait.until(() -> {
      assertEquals(Arrays.asList(3L, 1L), confirmed);
    });
  }
  
  /**
   *  Tests the condition where a new flow is created after the termination of the
   *  ShardedFlow container. In this case flows should be stillborn (pre-terminated).
   */
  @Test
  public void testTerminateThenCreate() {
    flow.terminate();
    final Ledger ledger = mock(Ledger.class);
    final MessageContext context = new DefaultMessageContext(ledger, null, flow);
    final Confirmation c = flow.begin(context, message(0, 0));
    c.confirm();
    Threads.sleep(10);
    verify(ledger, never()).confirm(any(), any());
  }

  private static Message message(long ballotId, int shard) {
    return new Proposal(String.valueOf(ballotId), new String[0], null, 0)
        .withShard(shard)
        .withMessageId(new DefaultMessageId(shard, ballotId));
  }
}
