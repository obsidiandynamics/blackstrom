package com.obsidiandynamics.blackstrom.worker;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.indigo.util.*;

public final class WorkerThreadTest {
  private static final int MAX_WAIT = 10_000;
  
  @Test
  public void testSingleRun() {
    final AtomicInteger counter = new AtomicInteger();
    final WorkerThread thread = WorkerThread.builder()
        .onCycle(t -> {
          counter.incrementAndGet();
          assertEquals(WorkerState.RUNNING, t.getState());
          t.terminate();
          assertEquals(WorkerState.TERMINATING, t.getState());
          t.terminate(); // second call should have no effect
          assertEquals(WorkerState.TERMINATING, t.getState());
        })
        .build();
    assertEquals(WorkerState.CONCEIVED, thread.getState());
    thread.start();
    
    final boolean joined = thread.joinQuietly(60_000);
    assertTrue(joined);
    assertEquals(1, counter.get());
    TestSupport.sleep(10);
    assertEquals(1, counter.get());
    assertFalse(thread.getDriver().isAlive());
    assertEquals(WorkerState.TERMINATED, thread.getState());
  }

  @Test
  public void testTerminateOnInterrupt() {
    final AtomicInteger counter = new AtomicInteger();
    final WorkerShutdown onShutdown = mock(WorkerShutdown.class);
    final WorkerThread thread = WorkerThread.builder()
        .onCycle(t -> {
          counter.incrementAndGet();
          throw new InterruptedException();
        })
        .onShutdown(onShutdown)
        .build();
    thread.start();
    thread.joinQuietly();
    
    assertEquals(WorkerState.TERMINATED, thread.getState());
    assertEquals(1, counter.get());
    assertFalse(thread.getDriver().isAlive());
    verify(onShutdown).handle(eq(thread), any(InterruptedException.class));
  }

  @Test
  public void testTerminateOnUnhandledException() {
    final WorkerShutdown onShutdown = mock(WorkerShutdown.class);
    final RuntimeException exception = new RuntimeException("Boom");
    final WorkerThread thread = WorkerThread.builder()
        .onCycle(t -> {
          throw exception;
        })
        .onShutdown(onShutdown)
        .build();
    thread.start();
    thread.joinQuietly();
    
    assertEquals(WorkerState.TERMINATED, thread.getState());
    assertFalse(thread.getDriver().isAlive());
    verify(onShutdown).handle(eq(thread), eq(exception));
  }
  
  @Test
  public void testLifecycleEvents() {
    final WorkerStartup onStartup = mock(WorkerStartup.class);
    final WorkerShutdown onShutdown = mock(WorkerShutdown.class);
    final WorkerThread thread = WorkerThread.builder()
        .onCycle(t -> Thread.sleep(10))
        .onStartup(onStartup)
        .onShutdown(onShutdown)
        .build();
    thread.start();
    
    Timesert.wait(MAX_WAIT).untilTrue(() -> thread.getState() == WorkerState.RUNNING);
    verify(onStartup).handle(eq(thread));
    thread.terminate().joinQuietly();
    verify(onShutdown).handle(eq(thread), any());
  }
  
  @Test(expected=IllegalStateException.class)
  public void testBuildWithWorker() {
    WorkerThread.builder().build();
  }
  
  @Test(expected=IllegalStateException.class)
  public void testStartTwice() {
    final WorkerThread thread = WorkerThread.builder()
        .onCycle(t -> t.terminate())
        .build();
    thread.start();
    thread.start(); // this call should throw an exception
  }
  
  @Test
  public void testToString() {
    final WorkerThread thread = WorkerThread.builder()
        .onCycle(t -> {})
        .build();
    Assertions.assertToStringOverride(thread);
  }
  
  @Test
  public void testJoinInterrupted() {
    final WorkerThread thread = WorkerThread.builder()
        .onCycle(t -> Thread.sleep(10))
        .build();
    thread.start();
    Thread.currentThread().interrupt();
    thread.joinQuietly();
    assertTrue(Thread.interrupted());
    
    thread.terminate().joinQuietly();
  }
  
  @Test
  public void testJoinTimeout() throws InterruptedException {
    final WorkerThread thread = WorkerThread.builder()
        .onCycle(t -> Thread.sleep(10))
        .build();
    thread.start();
    final boolean joined = thread.join(10);
    assertFalse(joined);
    
    thread.terminate().join();
  }
  
  @Test
  public void testOptions() {
    final WorkerOptions options = new WorkerOptions()
        .withName("TestThread")
        .withDaemon(true)
        .withPriority(3);
    final WorkerThread thread = WorkerThread.builder()
        .withOptions(options)
        .onCycle(t -> {})
        .build();
    assertEquals(options.getName(), thread.getName());
    assertEquals(options.isDaemon(), thread.isDaemon());
    assertEquals(options.getPriority(), thread.getPriority());
    Assertions.assertToStringOverride(options);
  }
  
  @Test
  public void testEqualsHashCode() {
    final WorkerThread t1 = WorkerThread.builder()
        .onCycle(t -> {})
        .build();
    final WorkerThread t2 = WorkerThread.builder()
        .onCycle(t -> {})
        .build();
    final WorkerThread t3 = t1;
    final Object t4 = new Object();
    assertFalse(t1.equals(t2));
    assertTrue(t1.equals(t3));
    assertFalse(t1.equals(t4));
    assertTrue(t1.hashCode() == t3.hashCode());
  }
}
