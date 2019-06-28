package com.obsidiandynamics.blackstrom.initiator;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.zerolog.*;

public final class AsyncInitiatorTest {
  private Manifold manifold;

  @After
  public void after() {
    if (manifold != null) {
      manifold.dispose();
    }
  }

  @Test
  public void testQuery_withFuture() throws Exception {
    final var logTarget = new MockLogTarget();
    final var initiator = new AsyncInitiator().withZlg(logTarget.logger());
    final var called = new AtomicInteger();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onQuery((c, m) -> {
                      called.incrementAndGet();
                      c.getLedger().append(new QueryResponse(m.getXid(), "done"));
                    })
                    .build())
        .build();

    final var query = new Query("X0", "do", 0);
    final var res = initiator.initiate(query).get();
    initiator.cancel(query.getXid()); // should do nothing
    assertNotNull(res);
    assertEquals("done", res.getResult());
    assertEquals(1, called.get());
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.TRACE).containing("Matched response QueryResponse").assertCount(1);
  }

  @Test(expected=TimeoutException.class)
  public void testQuery_withFutureTimeoutDueToCancel() throws Exception {
    final var initiator = new AsyncInitiator();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onQuery((c, m) -> {
                      initiator.cancel(m);
                      c.getLedger().append(new QueryResponse(m.getXid(), "done"));
                    })
                    .build())
        .build();

    final var query = new Query("X0", "do", 0);
    final var resFuture = initiator.initiate(query);
    resFuture.get(10, TimeUnit.MILLISECONDS);
    assertFalse(initiator.isPending(query));
  }

  @Test(expected=TimeoutException.class)
  public void testQuery_withFutureTimeoutDueToNoResponse() throws Exception {
    final var initiator = new AsyncInitiator();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onQuery((c, m) -> {})
                    .build())
        .build();

    final var query = new Query("X0", "do", 0);
    final var resFuture = initiator.initiate(query);
    try {
      resFuture.get(10, TimeUnit.MILLISECONDS);
    } finally {
      assertTrue(initiator.isPending(query));
    }
  }

  @Test
  public void testQuery_withResponseCallback() throws Exception {
    final var initiator = new AsyncInitiator();
    final var called = new AtomicInteger();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onQuery((c, m) -> {
                      called.incrementAndGet();
                      c.getLedger().append(new QueryResponse(m.getXid(), "done"));
                    })
                    .build())
        .build();

    final var callback = Classes.<Consumer<QueryResponse>>cast(mock(Consumer.class));
    initiator.initiate(new Query("X0", "do", 0), callback);
    Wait.SHORT.until(() -> {
      final var captor = ArgumentCaptor.forClass(QueryResponse.class);
      verify(callback).accept(captor.capture());
      final var res = captor.getValue();
      assertNotNull(res);
      assertEquals("done", res.getResult());
      assertEquals(1, called.get());
    });
  }

  @Test
  public void testCommand_withFuture() throws Exception {
    final var initiator = new AsyncInitiator();
    final var called = new AtomicInteger();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onCommand((c, m) -> {
                      called.incrementAndGet();
                      c.getLedger().append(new CommandResponse(m.getXid(), "done"));
                    })
                    .build())
        .build();

    final var command = new Command("X0", "do", 0);
    final var res = initiator.initiate(command).get();
    assertNotNull(res);
    assertEquals("done", res.getResult());
    assertEquals(1, called.get());
    assertFalse(initiator.isPending(command));
  }

  @Test
  public void testCommand_withResponseCallback() throws Exception {
    final var initiator = new AsyncInitiator();
    final var called = new AtomicInteger();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onCommand((c, m) -> {
                      called.incrementAndGet();
                      c.getLedger().append(new CommandResponse(m.getXid(), "done"));
                    })
                    .build())
        .build();

    final var callback = Classes.<Consumer<CommandResponse>>cast(mock(Consumer.class));
    initiator.initiate(new Command("X0", "do", 0), callback);
    Wait.SHORT.until(() -> {
      final var captor = ArgumentCaptor.forClass(CommandResponse.class);
      verify(callback).accept(captor.capture());
      final var res = captor.getValue();
      assertNotNull(res);
      assertEquals("done", res.getResult());
      assertEquals(1, called.get());
    });
  }

  @Test
  public void testProposal_withFuture() throws Exception {
    final var logTarget = new MockLogTarget();
    final var initiator = new AsyncInitiator().withZlg(logTarget.logger());
    final var called = new AtomicInteger();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onProposal((c, m) -> {
                      called.incrementAndGet();
                      c.getLedger().append(new Outcome(m.getXid(), Resolution.COMMIT, null, new Response[0], null));

                      // second append should do nothing
                      c.getLedger().append(new Outcome(m.getXid(), Resolution.ABORT, AbortReason.REJECT, new Response[0], null));
                    })
                    .build())
        .build();

    final var outcome = initiator.initiate(new Proposal("X0", new String[0], null, 0)).get();
    assertNotNull(outcome);
    assertEquals(Resolution.COMMIT, outcome.getResolution());
    assertEquals(1, called.get());
    logTarget.entries().forLevel(LogLevel.TRACE).containing("Matched response Outcome").assertCount(1);
    Wait.SHORT.until(() -> {
      logTarget.entries().assertCount(2);
      logTarget.entries().forLevel(LogLevel.TRACE).containing("Unmatched response Outcome").assertCount(1);
    });
  }

  @Test
  public void testProposal_withResponseCallback() throws Exception {
    final var initiator = new AsyncInitiator();
    final var called = new AtomicInteger();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onProposal((c, m) -> {
                      called.incrementAndGet();
                      c.getLedger().append(new Outcome(m.getXid(), Resolution.COMMIT, null, new Response[0], null));

                      // second append should do nothing
                      c.getLedger().append(new Outcome(m.getXid(), Resolution.ABORT, AbortReason.REJECT, new Response[0], null));
                    })
                    .build())
        .build();

    final var callback = Classes.<Consumer<Outcome>>cast(mock(Consumer.class));
    initiator.initiate(new Proposal("X0", new String[0], null, 0), callback);
    Wait.SHORT.until(() -> {
      final var captor = ArgumentCaptor.forClass(Outcome.class);
      verify(callback).accept(captor.capture());
      final var outcome = captor.getValue();
      assertNotNull(outcome);
      assertEquals(Resolution.COMMIT, outcome.getResolution());
      assertEquals(1, called.get());
    });
  }

  @Test
  public void testProposal_withResponseAndAppendCallbacks() throws Exception {
    final var initiator = new AsyncInitiator();
    final var called = new AtomicInteger();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onProposal((c, m) -> {
                      called.incrementAndGet();
                      c.getLedger().append(new Outcome(m.getXid(), Resolution.COMMIT, null, new Response[0], null));

                      // second append should do nothing
                      c.getLedger().append(new Outcome(m.getXid(), Resolution.ABORT, AbortReason.REJECT, new Response[0], null));
                    })
                    .build())
        .build();

    final var responseCallback = Classes.<Consumer<Outcome>>cast(mock(Consumer.class));
    final var appendCallback = mock(AppendCallback.class);
    initiator.initiate(new Proposal("X0", new String[0], null, 0), responseCallback, appendCallback);
    Wait.SHORT.until(() -> {
      final var captor = ArgumentCaptor.forClass(Outcome.class);
      verify(responseCallback).accept(captor.capture());
      final var outcome = captor.getValue();
      assertNotNull(outcome);
      assertEquals(Resolution.COMMIT, outcome.getResolution());
      assertEquals(1, called.get());
      
      verify(appendCallback).onAppend(any(), isNull());
    });
  }
}
