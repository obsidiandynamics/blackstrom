package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class BalancedLedgerHubTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final Timesert wait = Wait.SHORT;
  
  private BalancedLedgerHub hub;
  
  @After
  public void after() {
    if (hub != null) hub.dispose();
  }
  
  private static class TestView {
    final int viewId;
    final BalancedLedgerView view;
    final List<TestHandler> handlers = new ArrayList<>();
    
    private TestView(int viewId, BalancedLedgerView view) {
      this.viewId = viewId;
      this.view = view;
    }
    
    static TestView connectTo(int viewId, BalancedLedgerHub hub) {
      return new TestView(viewId, hub.connect());
    }
    
    void attach(String groupId) {
      final TestHandler handler = new TestHandler(groupId);
      handlers.add(handler);
      view.attach(handler);
    }
    
    void confirmFirst() {
      handlers.forEach(handler -> handler.confirmFirst());
    }
    
    void confirmLast() {
      handlers.forEach(handler -> handler.confirmLast());
    }
    
    void clear() {
      handlers.forEach(handler -> handler.clear());
    }
    
    class TestHandler implements MessageHandler {
      final List<List<Long>> receivedByShard;
      final List<AtomicReference<Message>> firstMessageByShard;
      final List<AtomicReference<Message>> lastMessageByShard;
      final String groupId;
      private volatile MessageContext context;
      
      private TestHandler(String groupId) {
        this.groupId = groupId;
        receivedByShard = IntStream.range(0, view.getHub().getShards())
            .boxed().map(shard -> new CopyOnWriteArrayList<Long>()).collect(Collectors.toList());
        firstMessageByShard = IntStream.range(0, view.getHub().getShards())
            .boxed().map(shard -> new AtomicReference<Message>()).collect(Collectors.toList());
        lastMessageByShard = IntStream.range(0, view.getHub().getShards())
            .boxed().map(shard -> new AtomicReference<Message>()).collect(Collectors.toList());
      }

      @Override
      public String getGroupId() {
        return groupId;
      }

      @Override
      public void onMessage(MessageContext context, Message message) {
        TestSupport.logStatic("%d-%x got %s\n", viewId, System.identityHashCode(this),  message);
        this.context = context;
        receivedByShard.get(message.getShard()).add((Long) message.getBallotId());
        firstMessageByShard.get(message.getShard()).compareAndSet(null, message);
        lastMessageByShard.get(message.getShard()).set(message);
      }
      
      private void confirm(List<AtomicReference<Message>> messageRefs) {
        messageRefs.forEach(messageRef -> {
          Optional.ofNullable(messageRef.get()).map(m -> m.getMessageId()).ifPresent(id -> context.confirm(id));
        });
      }
      
      void confirmFirst() {
        confirm(firstMessageByShard);
      }
      
      void confirmLast() {
        confirm(lastMessageByShard);
      }
      
      void clear() {
        context = null;
        receivedByShard.forEach(received -> received.clear());
        firstMessageByShard.forEach(messageRef -> messageRef.set(null));
        lastMessageByShard.forEach(messageRef -> messageRef.set(null));
      }
    }
  }

  @Test
  public void testUngroupedAll() {
    final int shards = 4;
    final int numViews = 2;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> {
          final TestView view = TestView.connectTo(v, hub);
          // attach before publishing -- should see all messages
          IntStream.range(0, handlers).forEach(h -> view.attach(null));
          return view;
        })
        .collect(Collectors.toList());
    
    final List<Long> expected = LongList.generate(0, messages);
    expected.forEach(ballotId -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(ballotId, 0).withShard(shard));
    }));
    
    wait.until(() -> {
      views.forEach(view -> view.handlers.forEach(handler -> handler.receivedByShard.forEach(received -> {
        assertEquals(expected, received);
      })));
    });
  }

  @Test
  public void testUngroupedNone() {
    final int shards = 4;
    final int numViews = 2;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> TestView.connectTo(v, hub))
        .collect(Collectors.toList());
    
    LongList.generate(0, messages).forEach(ballotId -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(ballotId, 0).withShard(shard));
    }));
    
    // attach after publishing -- shouldn't see any messages
    views.forEach(view -> IntStream.range(0, handlers).forEach(h -> {
      view.attach(null);
    }));
    
    TestSupport.sleep(10);
    
    views.forEach(view -> view.handlers.forEach(handler -> handler.receivedByShard.forEach(received -> {
      assertEquals(LongList.empty(), received);
    })));
  }

  @Test
  public void testGroupedCountdownNoConfirm() {
    final int shards = 4;
    final int numViews = 3;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> {
          final TestView view = TestView.connectTo(v, hub);
          IntStream.range(0, handlers).forEach(h -> view.attach("group"));
          return view;
        })
        .collect(Collectors.toList());
    
    final LongList expected = LongList.generate(0, messages);
    expected.forEach(ballotId -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(ballotId, 0).withShard(shard));
    }));
    
    // verify that we have exactly one receipt for each shard
    wait.until(assertAtLeastOneForEachShard(shards, views, expected));
    
    // one by one, dispose and remove each view and verify that the shards have rebalanced (i.e. there is still exactly
    // one receipt per shard), for as long as there is at least one remaining view
    IntStream.range(0, numViews).forEach(v -> {
      final TestView view = views.remove(0);
      view.view.dispose();
      wait.until(assertAtLeastOneForEachShard(shards, views, expected));
    });
  }

  @Test
  public void testGroupedCountdownConfirmFirst() {
    final int shards = 4;
    final int numViews = 3;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> {
          final TestView view = TestView.connectTo(v, hub);
          IntStream.range(0, handlers).forEach(h -> view.attach("group"));
          return view;
        })
        .collect(Collectors.toList());
    
    final LongList expected = LongList.generate(0, messages);
    expected.forEach(ballotId -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(ballotId, 0).withShard(shard));
    }));
    
    // verify that we have exactly one receipt for each shard
    wait.until(assertAtLeastOneForEachShard(shards, views, expected));
    
    // confirm at the head offset -- after rebalancing all messages will be redelivered
    views.forEach(view -> view.confirmFirst());
    
    // one by one, dispose and remove each view and verify that the shards have rebalanced (i.e. there is still exactly
    // one receipt per shard), for as long as there is at least one remaining view
    IntStream.range(0, numViews).forEach(v -> {
      final TestView view = views.remove(0);
      view.view.dispose();
      wait.until(assertAtLeastOneForEachShard(shards, views, expected));
    });
  }

  @Test
  public void testGroupedCountdownConfirmLast() {
    final int shards = 2;
    final int numViews = shards + 2;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> {
          final TestView view = TestView.connectTo(v, hub);
          IntStream.range(0, handlers).forEach(h -> view.attach("group"));
          return view;
        })
        .collect(Collectors.toList());
    
    LongList.generate(0, messages).forEach(ballotId -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(ballotId, 0).withShard(shard));
    }));
    
    // verify that we have exactly one full receipt for each shard
    final LongList expectedFull = LongList.generate(0, messages);
    wait.until(assertAtLeastOneForEachShard(shards, views, expectedFull));
    
    // confirm at the tail offset -- after rebalancing only the tail message will be redelivered
    views.forEach(view -> view.confirmLast());
    
    // remove views that have had complete receipts and clear the remaining views
    final List<TestView> completed = viewsWithAtLeastOne(views, expectedFull);
    views.forEach(view -> view.clear());
    completed.forEach(view -> {
      view.view.dispose();
      views.remove(view);
    });
    
    // one by one, dispose each view and verify that the shards have rebalanced, so 
    // that there is at least one tail message received (since we confirmed at the tail offset)
    final LongList expectedTail = LongList.generate(messages - 1, messages);
    IntStream.range(0, views.size() - 1).forEach(v -> {
      final TestView view = views.get(v);
      view.view.dispose();
      wait.until(assertAtLeastOneForEachShard(shards, views, expectedTail));
    });
  }
  
  @Test
  public void testHandover() {
    final int shards = 1;
    final int messages = 20;
    final int handlers = 2;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10));
    
    final TestView v0 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v0.attach("group"));
    final LongList expected = LongList.generate(0, messages);
    expected.forEach(ballotId -> IntStream.range(0, shards).forEach(shard -> {
      v0.view.append(new UnknownMessage(ballotId, 0).withShard(shard));
    }));
    wait.until(assertAtLeastOneForEachShard(1, Collections.singletonList(v0), expected));
    
    v0.view.dispose();
    final TestView v1 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v1.attach("group"));
    v1.attach("group");
    wait.until(assertAtLeastOneForEachShard(1, Collections.singletonList(v1), expected));
  }
  
  private static List<TestView> viewsWithAtLeastOne(List<TestView> views, LongList expected) {
    return views.stream().filter(view -> {
      final long receiptsInView = view.handlers.stream().map(handler -> {
        return handler.receivedByShard.stream()
            .filter(received -> received.equals(expected)).count();
      }).collect(Collectors.summingLong(i -> i));
      return receiptsInView > 0;
    }).collect(Collectors.toList());
  }
  
  private static Runnable assertNoneForEachShard(int shards, List<TestView> views, LongList expected) {
    return assertForEachShard(shards, views, expected, 0, 0);
  }
  
  private static Runnable assertAtLeastOneForEachShard(int shards, List<TestView> views, LongList expected) {
    return assertForEachShard(shards, views, expected, 1, Integer.MAX_VALUE);
  }

//  private static Runnable assertAtMostOneForEachShard(int shards, List<TestView> views, LongList expected) {
//    return assertForEachShard(shards, views, expected, 0, 1);
//  }
//
//  private static Runnable assertExactlyOneForEachShard(int shards, List<TestView> views, LongList expected) {
//    return assertForEachShard(shards, views, expected, 1, 1);
//  }
  
  /**
   *  Verifies that each shard's message content has been received by some bounded number of handlers (across all
   *  views), for as long at least one view remains.
   *  
   *  @param shards
   *  @param views
   *  @param expected
   *  @param minCount
   *  @param maxCount
   *  @return
   */
  private static Runnable assertForEachShard(int shards, List<TestView> views, LongList expected, int minCount, int maxCount) {
    return () -> {
      if (views.isEmpty()) return;
      
      IntStream.range(0, shards).forEach(shard -> {
        // count the number of handlers across all views that have received something for this shard
        final long numPartialReceipts = views.stream().map(view -> {
          return view.handlers.stream()
              .filter(handler -> ! handler.receivedByShard.get(shard).isEmpty()).count();
        }).collect(Collectors.summingLong(i -> i));
        assertTrue("numPartialReceipts=" + numPartialReceipts, numPartialReceipts >= minCount);
        assertTrue("numPartialReceipts=" + numPartialReceipts, numPartialReceipts <= maxCount);
        
        // count the number of handlers accross all view that have received the full list of expected 
        // messages for this shard
        final long numFullReceipts = views.stream().map(view -> {
          return view.handlers.stream()
              .filter(handler -> handler.receivedByShard.get(shard).equals(expected)).count();
        }).collect(Collectors.summingLong(i -> i));
        assertTrue("numFullReceipts=" + numFullReceipts, numFullReceipts >= minCount);
        assertTrue("numFullReceipts=" + numFullReceipts, numFullReceipts <= maxCount);
      });
    };
  }
}
