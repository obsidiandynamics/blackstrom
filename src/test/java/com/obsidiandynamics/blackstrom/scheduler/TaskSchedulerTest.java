package com.obsidiandynamics.blackstrom.scheduler;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
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
    return TestCycle.timesQuietly(1);
  }
  
  private static final int MAX_WAIT = 10_000;
  
  private static final class TestTask extends AbstractTask<UUID> {
    private final Consumer<TestTask> taskBody;
    
    TestTask(long time, UUID id, Consumer<TestTask> taskBody) {
      super(time, id);
      this.taskBody = taskBody;
    }
    
    @Override
    public void execute(TaskScheduler scheduler) {
      taskBody.accept(this);
    }
  }
  
  private static final class Receiver {
    private final List<UUID> ids = new CopyOnWriteArrayList<>();
    
    void receive(TestTask task) {
      ids.add(task.getId());
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
    scheduler.terminate().joinQuietly();
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
    // park the scheduler until we're ready to execute
    final CyclicBarrier barrier = new CyclicBarrier(2);
    scheduler.schedule(new AbstractTask<Long>(0, -1L) {
      @Override public void execute(TaskScheduler scheduler) {
        TestSupport.await(barrier);
      }
    });
    
    final List<UUID> ids = new ArrayList<>(tasks);  
    final long referenceNanos = System.nanoTime();
    for (int i = tasks; --i >= 0; ) {
      final TestTask task = doIn(new UUID(0, i), referenceNanos, i);
      ids.add(task.getId());
      scheduler.schedule(task);
    }
    
    TestSupport.await(barrier); // resume scheduling
   
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
  public void testAbortWhileExecute() {
    final int runs = 10;
    for (int i = 0; i < runs; i++) {
      final long time = System.nanoTime() + i * 1_000_000L;
      final Task task = new Task() {
        private boolean aborting;
        
        @Override
        public long getTime() {
          if (Thread.currentThread().getName().startsWith("TaskScheduler-") && ! aborting) {
            aborting = true;
            scheduler.abort(this);
          }
          return time;
        }
  
        @Override
        public Comparable<?> getId() {
          return 0;
        }
  
        @Override
        public void execute(TaskScheduler scheduler) {}
      };
      scheduler.schedule(task);
    }
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
  
  @Test
  public void testScheduleVolumeBenchmark() {
    testScheduleVolumeBenchmark(10_000,
                                Math.min(Runtime.getRuntime().availableProcessors(), 4),
                                10_000_000,
                                50_000_000);
  }
  
  private void testScheduleVolumeBenchmark(int tasks, int submissionThreads, long minDelayNanos, long maxDelayNanos) {
    final AtomicInteger fired = new AtomicInteger();
    final Consumer<TestTask> counter = tt -> fired.incrementAndGet();
    final long startNanos = System.nanoTime();

    final int tasksPerThread = tasks / submissionThreads;
    ParallelJob.nonBlocking(submissionThreads, threadIdx -> {
      for (int i = 0; i < tasksPerThread; i++) {
        final long delayNanos = (long) (Math.random() * (maxDelayNanos - minDelayNanos)) + minDelayNanos;
        final TestTask task = new TestTask(startNanos + delayNanos, 
                                           new UUID(threadIdx, i),
                                           counter);
        scheduler.schedule(task);
      }
    }).run();
   
    final int expectedTasks = tasksPerThread * submissionThreads;
    Timesert.wait(MAX_WAIT).untilTrue(() -> fired.get() == expectedTasks);
    final long took = System.nanoTime() - startNanos - minDelayNanos;

    System.out.format("Schedule volume: %,d took %,d ms, %,.0f tasks/sec (%d threads)\n", 
                      expectedTasks, took / 1_000_000L, (double) expectedTasks / took * 1_000_000_000L, submissionThreads);
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
                        receiver::receive);
  }
}
