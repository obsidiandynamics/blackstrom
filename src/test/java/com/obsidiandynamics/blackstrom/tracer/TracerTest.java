package com.obsidiandynamics.blackstrom.tracer;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

public final class TracerTest {
  private final Timesert wait = Wait.SHORT;

  private Tracer tracer;

  @After
  public void after() {
    if (tracer != null) tracer.terminate().joinQuietly();
  }

  private void createTracer(FiringStrategyFactory firingStrategyFactory) {
    tracer = new Tracer(firingStrategyFactory);
  }

  private static class TestTask implements Runnable {
    private final List<Integer> list;
    private final int taskId;

    TestTask(List<Integer> list, int taskId) {
      this.list = list;
      this.taskId = taskId;
    }

    @Override
    public void run() {
      list.add(taskId);
    }

    @Override
    public String toString() {
      return TestTask.class.getSimpleName() + " [taskId=" + taskId + "]";
    }
  }

  @Test
  public void testStrictNoComplete() {
    createTracer(StrictFiringStrategy::new);
    final int runs = 10;
    final List<Integer> completed = new CopyOnWriteArrayList<>();

    for (int i = 0; i < runs; i++) {
      tracer.begin(new TestTask(completed, i));
    }

    TestSupport.sleep(10);
    assertEquals(0, completed.size());
  }

  @Test
  public void testStrictIncreasing() {
    createTracer(StrictFiringStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);

    expected.forEach(i -> actions.add(tracer.begin(new TestTask(completed, i))));
    actions.forEach(a -> a.complete());

    wait.until(ListQuery.of(completed).isSize(runs));
    assertEquals(expected, completed);
  }

  @Test
  public void testStrictDecreasing() {
    createTracer(StrictFiringStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);

    expected.forEach(i -> actions.add(tracer.begin(new TestTask(completed, i))));
    ListQuery.of(actions).transform(Collections::reverse).list().forEach(a -> a.complete());

    wait.until(ListQuery.of(completed).isSize(runs));
    assertEquals(expected, completed);
  }

  @Test
  public void testStrictRandom() {
    createTracer(StrictFiringStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);

    expected.forEach(i -> actions.add(tracer.begin(new TestTask(completed, i))));
    ListQuery.of(actions).transform(Collections::shuffle).list().forEach(a -> a.complete());

    wait.until(ListQuery.of(completed).isSize(runs));
    assertEquals(expected, completed);
  }

  @Test
  public void testLazyNoComplete() {
    createTracer(LazyFiringStrategy::new);
    final int runs = 10;
    final List<Integer> completed = new CopyOnWriteArrayList<>();

    for (int i = 0; i < runs; i++) {
      tracer.begin(new TestTask(completed, i));
    }

    TestSupport.sleep(10);
    assertEquals(0, completed.size());
  }

  @Test
  public void testLazyIncreasing() {
    createTracer(LazyFiringStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);

    expected.forEach(i -> actions.add(tracer.begin(new TestTask(completed, i))));
    ListQuery.of(actions).delayedBy(1).forEach(a -> a.complete());

    wait.until(ListQuery.of(completed).contains(runs - 1));
    assertThat(ListQuery.of(completed).isOrderedBy(Integer::compare));
  }

  @Test
  public void testLazyDecreasing() {
    createTracer(LazyFiringStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);

    expected.forEach(i -> actions.add(tracer.begin(new TestTask(completed, i))));
    ListQuery.of(actions).transform(Collections::reverse).list().forEach(a -> a.complete());

    wait.until(ListQuery.of(completed).contains(runs - 1));
    assertEquals(1, completed.size());
  }

  @Test
  public void testLazyRandom() {
    createTracer(LazyFiringStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);

    expected.forEach(i -> actions.add(tracer.begin(new TestTask(completed, i))));
    ListQuery.of(actions).transform(Collections::shuffle).delayedBy(1).forEach(a -> a.complete());

    wait.until(ListQuery.of(completed).contains(runs - 1));
    assertThat(ListQuery.of(completed).isOrderedBy(Integer::compare));
  }

  private static List<Integer> increasingListOf(int numElements) {
    final List<Integer> nums = new ArrayList<>(numElements);
    IntStream.range(0, numElements).forEach(nums::add);
    return nums;
  }
  
  private static void assertThat(Runnable assertion) {
    assertion.run();
  }

  private static class ListQuery<T> {
    private final List<T> list;

    private ListQuery(List<T> list) {
      this.list = list;
    }

    static <T> ListQuery<T> of(List<T> list) {
      return new ListQuery<T>(list);
    }

    Runnable isSize(int numberOfElements) {
      return () -> assertEquals(numberOfElements, list.size());
    }
    
    Runnable contains(T element) {
      return () -> assertTrue("element " + element + " missing from list " + list, list.contains(element));
    }
    
    Runnable isOrderedBy(Comparator<T> comparator) {
      final List<T> ordered = transform(l -> Collections.sort(l, comparator)).list;
      return () -> assertEquals(ordered, list);
    }
    
    List<T> list() {
      return list;
    }

    ListQuery<T> transform(Consumer<List<T>> transform) {
      final List<T> copy = new ArrayList<>(list);
      transform.accept(copy);
      return new ListQuery<>(copy);
    }
    
    DelayedLoop delayedBy(int delayMillis) {
      return new DelayedLoop(delayMillis);
    }
    
    class DelayedLoop {
      private final int delayMillis;

      DelayedLoop(int delayMillis) {
        this.delayMillis = delayMillis;
      }
      
      void forEach(Consumer<T> consumer) {
        list.forEach(t -> {
          if (delayMillis != 0) TestSupport.sleep(delayMillis); else Thread.yield();
          consumer.accept(t);
        });
      }
    }
  }
}
