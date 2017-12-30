package com.obsidiandynamics.blackstrom.worker;

import static junit.framework.TestCase.*;

import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.indigo.util.*;

public final class WorkerTest {
  @Test
  public void testSingleRun() {
    final AtomicInteger counter = new AtomicInteger();
    final WorkerThread thread = new WorkerThread(new WorkerOptions(), t -> {
      counter.incrementAndGet();
      assertEquals(WorkerState.RUNNING, t.getState());
      t.terminate();
      assertEquals(WorkerState.TERMINATING, t.getState());
      t.terminate(); // second call should have no effect
      assertEquals(WorkerState.TERMINATING, t.getState());
    });
    assertEquals(WorkerState.CONCEIVED, thread.getState());
    thread.start();
    thread.joinQuietly();
    assertEquals(1, counter.get());
    TestSupport.sleep(10);
    assertEquals(1, counter.get());
    assertFalse(thread.getDriver().isAlive());
    assertEquals(WorkerState.TERMINATED, thread.getState());
  }

  @Test
  public void testTerminateOnInterrupt() {
    final AtomicInteger counter = new AtomicInteger();
    final WorkerThread thread = new WorkerThread(new WorkerOptions(), t -> {
      counter.incrementAndGet();
      throw new InterruptedException();
    });
    thread.start();
    thread.joinQuietly();
    assertEquals(1, counter.get());
    TestSupport.sleep(10);
    assertEquals(1, counter.get());
    assertFalse(thread.getDriver().isAlive());
  }
  
  @Test(expected=IllegalStateException.class)
  public void testStartTwice() {
    final WorkerThread thread = new WorkerThread(new WorkerOptions(), t -> {
      t.terminate();
    });
    thread.start();
    thread.start(); // this call should throw an exception
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new WorkerThread(new WorkerOptions(), t -> {}));
  }
  
  @Test
  public void testJoinInterrupted() {
    final WorkerThread thread = new WorkerThread(new WorkerOptions(), t -> {
      TestSupport.sleep(10);
    });
    thread.start();
    Thread.currentThread().interrupt();
    thread.joinQuietly();
    assertTrue(Thread.interrupted());
    
    thread.terminate();
    thread.joinQuietly();
  }
  
  @Test
  public void testOptions() {
    final WorkerOptions options = new WorkerOptions()
        .withName("TestThread")
        .withDaemon(true)
        .withPriority(3);
    final WorkerThread thread = WorkerThread.builder()
        .withOptions(options)
        .withWorker(t -> {})
        .build();
    assertEquals(options.getName(), thread.getName());
    assertEquals(options.isDaemon(), thread.isDaemon());
    assertEquals(options.getPriority(), thread.getPriority());
    Assertions.assertToStringOverride(options);
  }
  
  @Test
  public void testEqualsHashCode() {
    final WorkerThread t1 = new WorkerThread(new WorkerOptions(), t -> {});
    final WorkerThread t2 = new WorkerThread(new WorkerOptions(), t -> {});
    final WorkerThread t3 = t1;
    final Object t4 = new Object();
    assertFalse(t1.equals(t2));
    assertTrue(t1.equals(t3));
    assertFalse(t1.equals(t4));
    assertTrue(t1.hashCode() == t3.hashCode());
  }
}
