package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.assertj.core.api.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;

@RunWith(Parameterized.class)
public final class BalancedLedgerHubTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }

  private static final Zlg zlg = Zlg.forDeclaringClass().get();

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
      return new TestView(viewId, hub.connectDetached());
    }

    TestHandler attach(String groupId) {
      final TestHandler handler = new TestHandler(groupId);
      handlers.add(handler);
      view.attach(handler);
      return handler;
    }

    void confirmFirst() {
      handlers.forEach(TestHandler::confirmFirst);
    }

    void confirmLast() {
      handlers.forEach(TestHandler::confirmLast);
    }

    void clear() {
      handlers.forEach(TestHandler::clear);
    }

    class TestHandler implements MessageHandler {
      final List<List<Long>> receivedByShard;
      final List<AtomicReference<Message>> firstMessageByShard;
      final List<AtomicReference<Message>> lastMessageByShard;
      final String groupId;
      private volatile MessageContext context;
      private long pause;

      private TestHandler(String groupId) {
        this.groupId = groupId;
        receivedByShard = IntStream.range(0, view.getHub().getShards())
            .boxed().map(shard -> new CopyOnWriteArrayList<Long>()).collect(Collectors.toList());
        firstMessageByShard = IntStream.range(0, view.getHub().getShards())
            .boxed().map(shard -> new AtomicReference<Message>()).collect(Collectors.toList());
        lastMessageByShard = IntStream.range(0, view.getHub().getShards())
            .boxed().map(shard -> new AtomicReference<Message>()).collect(Collectors.toList());
      }

      TestHandler withPause(long pauseMillis) {
        this.pause = pauseMillis;
        return this;
      }

      @Override
      public String getGroupId() {
        return groupId;
      }

      @Override
      public void onMessage(MessageContext context, Message message) {
        zlg.t("%d-%x got %s", z -> z.arg(viewId).arg(System.identityHashCode(this)).arg(message));
        Threads.sleep(pause);
        this.context = context;
        receivedByShard.get(message.getShard()).add(Long.parseLong(message.getXid()));
        firstMessageByShard.get(message.getShard()).compareAndSet(null, message);
        lastMessageByShard.get(message.getShard()).set(message);
      }

      private void confirm(List<AtomicReference<Message>> messageRefs) {
        messageRefs.forEach(messageRef -> {
          Optional.ofNullable(messageRef.get())
          .ifPresent(m -> context.getLedger().confirm(context.getHandlerId(), m.getMessageId()));
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
        receivedByShard.forEach(List::clear);
        firstMessageByShard.forEach(messageRef -> messageRef.set(null));
        lastMessageByShard.forEach(messageRef -> messageRef.set(null));
      }
      
      @Override
      public String toString() {
        final var firstLastByShard = new ArrayList<String>(receivedByShard.size());
        for (var i = 0; i < receivedByShard.size(); i++) {
          final var first = firstMessageByShard.get(i).get();
          final var last = lastMessageByShard.get(i).get();
          firstLastByShard.add(ifPresent(first, Message::getMessageId) + "/" + ifPresent(last, Message::getMessageId));
        }
        return firstLastByShard.toString();
      }
    }
  }

  @Test
  public void testAttachedDispose() {
    hub = new BalancedLedgerHub(1, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 1));
    final BalancedLedgerView v0 = hub.connect();
    final BalancedLedgerView v1 = hub.connect();
    assertEquals(Set.of(v0, v1), hub.getViews());

    v0.dispose(); // this should also have the effect of disposing both the hub and v1
    assertEquals(Set.of(), hub.getViews());
  }

  @Test
  public void testDetachedDispose() {
    hub = new BalancedLedgerHub(1, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 1));
    final BalancedLedgerView v0 = hub.connectDetached();
    final BalancedLedgerView v1 = hub.connectDetached();
    assertEquals(Set.of(v0, v1), hub.getViews());

    v0.dispose(); // this should have no effect on the hub or v1
    assertEquals(Set.of(v1), hub.getViews());
  }

  @Test
  public void testConfirmAfterDispose() {
    hub = new BalancedLedgerHub(1, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 1));
    final BalancedLedgerView view = hub.connectDetached();
    final AtomicBoolean received = new AtomicBoolean();
    view.attach(new MessageHandler() {
      @Override
      public String getGroupId() {
        return null;
      }

      @Override
      public void onMessage(MessageContext context, Message message) {
        view.dispose();
        context.beginAndConfirm(message);
        received.set(true);
      }
    });
    view.append(new UnknownMessage("0", 0).withShard(0));

    wait.untilTrue(() -> received.get());
  }

  /**
   *  Ungrouped context. Messages are published after all the views have joined and each view
   *  receives the complete set of messages. 
   */
  @Test
  public void testUngroupedAll() {
    final int shards = 4;
    final int numViews = 2;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 2));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> {
          final TestView view = TestView.connectTo(v, hub);
          // attach before publishing — should see all messages
          IntStream.range(0, handlers).forEach(h -> view.attach(null));
          return view;
        })
        .collect(Collectors.toList());

    final List<Long> expected = LongList.generate(0, messages);
    expected.forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));

    wait.until(() -> {
      views.forEach(view -> view.handlers.forEach(handler -> handler.receivedByShard.forEach(received -> {
        assertEquals(expected, received);
      })));
    });
  }

  /**
   *  Ungrouped context. Messages are published before all the views have joined and none of the
   *  views receive any messages.
   */
  @Test
  public void testUngroupedNone() {
    final int shards = 4;
    final int numViews = 2;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 2));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> TestView.connectTo(v, hub))
        .collect(Collectors.toList());

    LongList.generate(0, messages).forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));

    // attach after publishing — shouldn't see any messages
    views.forEach(view -> IntStream.range(0, handlers).forEach(h -> {
      view.attach(null);
    }));

    Threads.sleep(10);

    views.forEach(view -> view.handlers.forEach(handler -> handler.receivedByShard.forEach(received -> {
      assertEquals(LongList.empty(), received);
    })));
  }

  /**
   *  Messages are published to a small buffer at a rate that is much higher than the consumer's 
   *  throughput, resulting in a buffer overflow condition.
   */
  @Test
  public void testBufferOverflow() {
    hub = new BalancedLedgerHub(1, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 1));

    final TestView view = TestView.connectTo(0, hub);
    view.attach(null).withPause(5); // simulate slow consumer

    final MockLogTarget logTarget = new MockLogTarget();
    view.view.withZlg(logTarget.logger());

    // first publish 5 messages and assert receipt
    LongList.generate(0, 5).forEach(xid -> {
      view.view.append(new UnknownMessage(String.valueOf(xid), 0));
    });

    wait.until(() -> {
      assertEquals(5, view.handlers.get(0).receivedByShard.get(0).size());
    });

    // publish more messages — some will be missed due to constrained buffer capacity
    LongList.generate(0, 100).forEach(xid -> {
      view.view.append(new UnknownMessage(String.valueOf(xid), 0));
    });

    wait.until(() -> {
      logTarget.entries().forLevel(LogLevel.WARN).containing("overflow").assertCountAtLeast(1);
    });
  }

  /**
   *  Messages are consumed from one of a set of contending views without confirming messages. 
   *  Contending views are disposed one by one and in each case messages are consumed again
   *  from one of the remaining views.
   */
  @Test
  public void testGroupedCountdownNoConfirm() {
    final int shards = 4;
    final int numViews = 3;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 2));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> {
          final TestView view = TestView.connectTo(v, hub);
          IntStream.range(0, handlers).forEach(h -> view.attach("group"));
          return view;
        })
        .collect(Collectors.toList());

    final LongList expected = LongList.generate(0, messages);
    expected.forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));

    // verify that we have exactly one receipt for each shard
    wait.until(assertExactlyOneForEachShard(shards, views, expected));

    // one by one, dispose and remove each view and verify that the shards have rebalanced (i.e. there is still exactly
    // one receipt per shard), for as long as there is at least one remaining view
    IntStream.range(0, numViews).forEach(v -> {
      final TestView view = views.remove(0);
      view.view.dispose();
      wait.until(assertAtLeastOneForEachShard(shards, views, expected));
    });
  }

  /**
   *  Messages are consumed from one of a set of contending views, confirming the last message. 
   *  Contending views are disposed one by one and in each case no further messages are consumed
   *  from the remaining views.
   */
  @Test
  public void testGroupedCountdownConfirmLast() {
    final int shards = 2;
    final int numViews = shards + 2;
    final int handlers = 2;
    final int messages = 20;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 2));
    final List<TestView> views = IntStream.range(0, numViews).boxed()
        .map(v -> {
          final TestView view = TestView.connectTo(v, hub);
          IntStream.range(0, handlers).forEach(h -> view.attach("group"));
          return view;
        })
        .collect(Collectors.toList());

    LongList.generate(0, messages).forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      views.get(0).view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));

    // verify that we have exactly one full receipt for each shard
    final LongList expectedFull = LongList.generate(0, messages);
    wait.until(assertExactlyOneForEachShard(shards, views, expectedFull));

    // confirm at the tail offset — after rebalancing only the tail message will be redelivered
    views.forEach(TestView::confirmLast);

    // remove views that have had complete receipts and clear the remaining views
    final List<TestView> completed = viewsWithAtLeastOne(views, expectedFull);
    completed.forEach(view -> {
      view.view.dispose();
      views.remove(view);
    });
    views.forEach(TestView::clear);

    // one by one, dispose each view and verify that no further messages have been received
    for (var view : views) {
      view.view.dispose();
      assertNoMessages(views);
    }
  }

  /**
   *  Tests handover using {@link RandomShardAssignment} with no confirmations.
   */
  @Test
  public void testHandoverRandomNoConfirm() {
    final int shards = 1;
    final int messages = 20;
    final int handlers = 2;
    hub = new BalancedLedgerHub(shards, RandomShardAssignment::new, ArrayListAccumulator.factory(10, 2));

    final TestView v0 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v0.attach("group"));
    final LongList expected = LongList.generate(0, messages);
    expected.forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      v0.view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));
    wait.until(assertAtLeastOneForEachShard(1, Collections.singletonList(v0), expected));

    v0.view.dispose();
    final TestView v1 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v1.attach("group"));
    wait.until(assertAtLeastOneForEachShard(1, Collections.singletonList(v1), expected));
  }

  /**
   *  Tests handover using {@link StickyShardAssignment} with no confirmations.
   */
  @Test
  public void testHandoverStickyNoConfirm() {
    final int shards = 1;
    final int messages = 20;
    final int handlers = 2;
    hub = new BalancedLedgerHub(shards, StickyShardAssignment::new, ArrayListAccumulator.factory(10, 2));

    final TestView v0 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v0.attach("group"));
    final LongList expected = LongList.generate(0, messages);
    expected.forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      v0.view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));
    wait.until(assertExactlyOneForEachShard(1, Collections.singletonList(v0), expected));

    final TestView v1 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v1.attach("group"));
    Threads.sleep(10);
    assertNoneForEachShard(1, Collections.singletonList(v1), expected).run();

    v0.view.dispose();
    wait.until(assertExactlyOneForEachShard(1, Collections.singletonList(v1), expected));
  }

  /**
   *  Tests handover using {@link StickyShardAssignment} with confirmation of the first
   *  message.
   */
  @Test
  public void testHandoverStickyConfirmFirst() {
    final int shards = 1;
    final int messages = 20;
    final int handlers = 2;
    hub = new BalancedLedgerHub(shards, StickyShardAssignment::new, ArrayListAccumulator.factory(10, 2));

    final TestView v0 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v0.attach("group"));
    final LongList expectedFor0 = LongList.generate(0, messages);
    expectedFor0.forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      v0.view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));
    wait.until(assertExactlyOneForEachShard(1, Collections.singletonList(v0), expectedFor0));

    final TestView v1 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v1.attach("group"));
    Threads.sleep(10);
    assertNoneForEachShard(1, Collections.singletonList(v1), expectedFor0).run();

    v0.confirmFirst();
    v0.view.dispose();
    final LongList expectedFor1 = LongList.generate(1, messages);
    wait.until(assertExactlyOneForEachShard(1, Collections.singletonList(v1), expectedFor1));
  }

  /**
   *  Tests handover using {@link StickyShardAssignment} with confirmation of the last
   *  message.
   */
  @Test
  public void testHandoverStickyConfirmLast() {
    final int shards = 1;
    final int messages = 20;
    final int handlers = 2;
    hub = new BalancedLedgerHub(shards, StickyShardAssignment::new, ArrayListAccumulator.factory(10, 2));

    final TestView v0 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v0.attach("group"));
    final LongList expectedFull = LongList.generate(0, messages);
    expectedFull.forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      v0.view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));
    wait.until(assertExactlyOneForEachShard(1, Collections.singletonList(v0), expectedFull));

    final TestView v1 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v1.attach("group"));
    Threads.sleep(10);
    assertNoneForEachShard(1, Collections.singletonList(v1), expectedFull).run();

    v0.confirmLast();
    v0.view.dispose();
    Threads.sleep(10);
    assertNoMessages(List.of(v1));
  }

  /**
   *  Tests consumption from two independent groups. Both will receive the same messages.
   */
  @Test
  public void testTwoGroups() {
    final int shards = 1;
    final int messages = 20;
    final int handlers = 2;
    hub = new BalancedLedgerHub(shards, StickyShardAssignment::new, ArrayListAccumulator.factory(10, 2));

    final TestView v0 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v0.attach("group-0"));
    final LongList expectedFull = LongList.generate(0, messages);
    expectedFull.forEach(xid -> IntStream.range(0, shards).forEach(shard -> {
      v0.view.append(new UnknownMessage(String.valueOf(xid), 0).withShard(shard));
    }));
    wait.until(assertExactlyOneForEachShard(1, Collections.singletonList(v0), expectedFull));

    final TestView v1 = TestView.connectTo(0, hub);
    IntStream.range(0, handlers).forEach(h -> v1.attach("group-1"));
    wait.until(assertExactlyOneForEachShard(1, Collections.singletonList(v1), expectedFull));
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
  
  private static void assertNoMessages(List<TestView> views) {
    for (var view : views) {
      for (var handler : view.handlers) {
        for (var received : handler.receivedByShard) {
          Assertions.assertThat(received).isEmpty();
        }
      }
    }
  }

  private static Runnable assertNoneForEachShard(int shards, List<TestView> views, LongList expected) {
    return assertForEachShard(shards, views, expected, 0, 0);
  }

  private static Runnable assertAtLeastOneForEachShard(int shards, List<TestView> views, LongList expected) {
    return assertForEachShard(shards, views, expected, 1, Integer.MAX_VALUE);
  }

  private static Runnable assertExactlyOneForEachShard(int shards, List<TestView> views, LongList expected) {
    return assertForEachShard(shards, views, expected, 1, 1);
  }

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
        // count the number of handlers across all view that have received the full list of expected 
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
