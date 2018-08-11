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

public final class AsyncInitiatorTest {
  private Manifold manifold;

  @After
  public void after() {
    if (manifold != null) {
      manifold.dispose();
    }
  }

  @Test
  public void testQueryWithFuture() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
    final AtomicInteger called = new AtomicInteger();
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

    final Query query = new Query("X0", "do", 0);
    final QueryResponse res = initiator.initiate(query).get();
    initiator.cancel(query.getXid()); // should do nothing
    assertNotNull(res);
    assertEquals("done", res.getResult());
    assertEquals(1, called.get());
  }

  @Test(expected=TimeoutException.class)
  public void testQueryWithFutureTimeoutDueToCancel() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
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

    final Query query = new Query("X0", "do", 0);
    final Future<QueryResponse> resFuture = initiator.initiate(query);
    resFuture.get(10, TimeUnit.MILLISECONDS);
    assertFalse(initiator.isPending(query));
  }

  @Test(expected=TimeoutException.class)
  public void testQueryWithFutureTimeoutDueToNoResponse() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
    manifold = Manifold.builder()
        .withLedger(new SingleNodeQueueLedger())
        .withFactor(initiator)
        .withFactor(LambdaCohort
                    .builder()
                    .onQuery((c, m) -> {})
                    .build())
        .build();

    final Query query = new Query("X0", "do", 0);
    final Future<QueryResponse> resFuture = initiator.initiate(query);
    try {
      resFuture.get(10, TimeUnit.MILLISECONDS);
    } finally {
      assertTrue(initiator.isPending(query));
    }
  }

  @Test
  public void testQueryWithResponseCallback() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
    final AtomicInteger called = new AtomicInteger();
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

    final Consumer<QueryResponse> callback = Classes.cast(mock(Consumer.class));
    initiator.initiate(new Query("X0", "do", 0), callback);
    Wait.SHORT.until(() -> {
      final ArgumentCaptor<QueryResponse> captor = ArgumentCaptor.forClass(QueryResponse.class);
      verify(callback).accept(captor.capture());
      final QueryResponse res = captor.getValue();
      assertNotNull(res);
      assertEquals("done", res.getResult());
      assertEquals(1, called.get());
    });
  }

  @Test
  public void testCommandWithFuture() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
    final AtomicInteger called = new AtomicInteger();
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

    final Command command = new Command("X0", "do", 0);
    final CommandResponse res = initiator.initiate(command).get();
    assertNotNull(res);
    assertEquals("done", res.getResult());
    assertEquals(1, called.get());
    assertFalse(initiator.isPending(command));
  }

  @Test
  public void testCommandWithResponseCallback() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
    final AtomicInteger called = new AtomicInteger();
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

    final Consumer<CommandResponse> callback = Classes.cast(mock(Consumer.class));
    initiator.initiate(new Command("X0", "do", 0), callback);
    Wait.SHORT.until(() -> {
      final ArgumentCaptor<CommandResponse> captor = ArgumentCaptor.forClass(CommandResponse.class);
      verify(callback).accept(captor.capture());
      final CommandResponse res = captor.getValue();
      assertNotNull(res);
      assertEquals("done", res.getResult());
      assertEquals(1, called.get());
    });
  }

  @Test
  public void testProposalWithFuture() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
    final AtomicInteger called = new AtomicInteger();
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

    final Outcome outcome = initiator.initiate(new Proposal("X0", new String[0], null, 0)).get();
    assertNotNull(outcome);
    assertEquals(Resolution.COMMIT, outcome.getResolution());
    assertEquals(1, called.get());
  }

  @Test
  public void testProposalWithResponseCallback() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
    final AtomicInteger called = new AtomicInteger();
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

    final Consumer<Outcome> callback = Classes.cast(mock(Consumer.class));
    initiator.initiate(new Proposal("X0", new String[0], null, 0), callback);
    Wait.SHORT.until(() -> {
      final ArgumentCaptor<Outcome> captor = ArgumentCaptor.forClass(Outcome.class);
      verify(callback).accept(captor.capture());
      final Outcome outcome = captor.getValue();
      assertNotNull(outcome);
      assertEquals(Resolution.COMMIT, outcome.getResolution());
      assertEquals(1, called.get());
    });
  }

  @Test
  public void testProposalWithResponseAndAppendCallbacks() throws Exception {
    final AsyncInitiator initiator = new AsyncInitiator();
    final AtomicInteger called = new AtomicInteger();
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

    final Consumer<Outcome> responseCallback = Classes.cast(mock(Consumer.class));
    final AppendCallback appendCallback = mock(AppendCallback.class);
    initiator.initiate(new Proposal("X0", new String[0], null, 0), responseCallback, appendCallback);
    Wait.SHORT.until(() -> {
      final ArgumentCaptor<Outcome> captor = ArgumentCaptor.forClass(Outcome.class);
      verify(responseCallback).accept(captor.capture());
      final Outcome outcome = captor.getValue();
      assertNotNull(outcome);
      assertEquals(Resolution.COMMIT, outcome.getResolution());
      assertEquals(1, called.get());
      
      verify(appendCallback).onAppend(any(), isNull());
    });
  }
}
