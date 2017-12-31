package com.obsidiandynamics.blackstrom.scheduler;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class TaskSchedulerTest implements TestSupport {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(2);
  }
  
  private static final int MAX_WAIT = 60_000;
  
  private static final class TestTask extends AbstractTask<UUID> {
    private final Receiver receiver;
    
    TestTask(long time, UUID id, Receiver receiver) {
      super(time, id);
      this.receiver = receiver;
    }
    
    @Override
    public void execute(TaskScheduler scheduler) {
      receiver.receive(getId());
    }
  }
  
  private static final class Receiver {
    private final List<UUID> ids = new CopyOnWriteArrayList<>();
    
    void receive(UUID id) {
      ids.add(id);
    }
    
    BooleanSupplier isSize(int size) {
      return () -> ids.size() == size;
    }
  }
  
  private Receiver receiver;
  
  private TaskScheduler scheduler;
  
  @Before
  public void setup() {
    receiver = new Receiver();
    scheduler = new TaskScheduler();
    scheduler.start();
  }
  
  @After
  public void teardown() throws InterruptedException {
    scheduler.terminate();
    scheduler.joinQuietly();
    scheduler.join(); // should be a no-op
  }
  
  @Test
  public void testSchedule() {
    testSchedule(10);
  }
  
  private void testSchedule(int tasks) {
    final List<UUID> ids = new ArrayList<>(tasks);
    for (int i = 0; i < tasks; i++) {
      final TestTask task = doIn(new UUID(0, i), i);
      ids.add(task.getId());
      scheduler.schedule(task);
    }
   
    Timesert.wait(MAX_WAIT).untilTrue(receiver.isSize(tasks));
    assertEquals(ids, receiver.ids);
  }
  
  @Test
  public void testScheduleReverse() {
    testScheduleReverse(10);
  }
  
  private void testScheduleReverse(int tasks) {
    TestSupport.sleep(10); // let the scheduler run at least once
    
    final List<UUID> ids = new ArrayList<>(tasks);  
    final long referenceNanos = System.nanoTime();
    for (int i = tasks; --i >= 0; ) {
      final TestTask task = doIn(new UUID(0, i), referenceNanos, 10 + i);
      ids.add(task.getId());
      scheduler.schedule(task);
    }
   
    Collections.reverse(ids);
    Timesert.wait(MAX_WAIT).untilTrue(receiver.isSize(tasks));
    assertEquals(ids, receiver.ids);
  }
  
  @Test
  public void testParallelSchedule() {
    testParallelSchedule(8, 10);
  }
  
  private void testParallelSchedule(int addThreads, int tasksPerThread) {
    ParallelJob.blocking(addThreads, threadIdx -> {
      final List<UUID> ids = new ArrayList<>(tasksPerThread);
      for (int i = 0; i < tasksPerThread; i++) {
        final TestTask task = doIn(new UUID(threadIdx, i), i);
        ids.add(task.getId());
        scheduler.schedule(task);
      }
    }).run();
    
    Timesert.wait(MAX_WAIT).untilTrue(receiver.isSize(addThreads * tasksPerThread));
  }
  
  @Test
  public void testClear() {
    testClear(10);
  }
  
  private void testClear(int tasks) {
    final List<UUID> ids = new ArrayList<>(tasks);
    for (int i = 0; i < tasks; i++) {
      final TestTask task = doIn(new UUID(0, i), i);
      ids.add(task.getId());
      scheduler.schedule(task);
    }
   
    scheduler.clear();
    TestSupport.sleep(10);
    assertTrue(receiver.ids.size() <= tasks);
  }
  
  @Test
  public void testForceExecute() {
    testForceExecute(10);
  }
  
  private void testForceExecute(int tasks) {
    final List<UUID> ids = new ArrayList<>(tasks); 
    for (int i = 0; i < tasks; i++) {
      final TestTask task = doIn(60_000 + i * 1_000);
      ids.add(task.getId());
      scheduler.schedule(task);
    }
   
    assertEquals(0, receiver.ids.size());
    scheduler.forceExecute();
    Timesert.wait(MAX_WAIT).untilTrue(receiver.isSize(tasks));
    assertEquals(ids, receiver.ids);
  }
  
  @Test
  public void testAbort() {
    testAbort(10);
  }
  
  private void testAbort(int tasks) {
    final List<TestTask> timeouts = new ArrayList<>(tasks); 
    for (int i = 0; i < tasks; i++) {
      final TestTask task = doIn(60_000 + i * 1_000);
      timeouts.add(task);
      scheduler.schedule(task);
    }
    
    assertEquals(0, receiver.ids.size());
    
    for (TestTask task : timeouts) {
      assertTrue(scheduler.abort(task));
      assertFalse(scheduler.abort(task)); // 2nd call should have no effect
    }
    
    assertEquals(0, receiver.ids.size());
    scheduler.forceExecute();
    assertEquals(0, receiver.ids.size());
  }

  @Test
  public void testEarlyExecute() {
    testEarlyExecute(10);
  }
  
  private void testEarlyExecute(int tasks) {
    final List<TestTask> timeouts = new ArrayList<>(tasks); 
    for (int i = 0; i < tasks; i++) {
      final TestTask task = doIn(new UUID(0, i), 60_000 + i * 1_000);
      timeouts.add(task);
      scheduler.schedule(task);
    }
   
    assertEquals(0, receiver.ids.size());
    for (TestTask task : timeouts) {
      scheduler.executeNow(task);
      scheduler.executeNow(task); // 2nd call should have no effect
    }
    assertEquals(tasks, receiver.ids.size());
    scheduler.forceExecute();
    assertEquals(tasks, receiver.ids.size());
    
    final List<UUID> sorted = new ArrayList<>(receiver.ids);
    Collections.sort(sorted);
    assertEquals(sorted, receiver.ids);
  }
  
  private TestTask doIn(long millis) {
    return doIn(UUID.randomUUID(), millis);
  }
  
  private TestTask doIn(UUID uuid, long millis) {
    return doIn(uuid, System.nanoTime(), millis);
  }
  
  private TestTask doIn(UUID uuid, long referenceNanos, long millis) {
    return new TestTask(referenceNanos + millis * 1_000_000l, 
                        uuid,
                        receiver);
  }
}