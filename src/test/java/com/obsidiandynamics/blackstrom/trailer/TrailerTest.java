package com.obsidiandynamics.blackstrom.trailer;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

public final class TrailerTest {
  private final Timesert wait = Wait.SHORT;
  
  private Trailer trailer;
  
  @After
  public void after() {
    if (trailer != null) trailer.terminate().joinQuietly();
  }
  
  private void createTrailer(CompletionStrategyFactory completionStrategyFactory) {
    trailer = new Trailer(completionStrategyFactory); 
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
    createTrailer(StrictCompletionStrategy::new);
    final int runs = 10;
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    
    for (int i = 0; i < runs; i++) {
      trailer.begin(new TestTask(completed, i));
    }
    
    TestSupport.sleep(10);
    assertEquals(0, completed.size());
  }
  
  @Test
  public void testStrictIncreasing() {
    createTrailer(StrictCompletionStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);
    
    expected.forEach(i -> actions.add(trailer.begin(new TestTask(completed, i))));
    actions.forEach(a -> a.complete());
    
    wait.until(Size.of(completed).is(runs));
    assertEquals(expected, completed);
  }
  
  @Test
  public void testStrictDecreasing() {
    createTrailer(StrictCompletionStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);
    
    expected.forEach(i -> actions.add(trailer.begin(new TestTask(completed, i))));
    Apply.to(actions).transform(Collections::reverse).forEach(a -> a.complete());
    
    wait.until(Size.of(completed).is(runs));
    assertEquals(expected, completed);
  }
  
  @Test
  public void testStrictRandom() {
    createTrailer(StrictCompletionStrategy::new);
    final int runs = 100;
    final List<Integer> expected = increasingListOf(runs);
    final List<Integer> completed = new CopyOnWriteArrayList<>();
    final List<Action> actions = new ArrayList<>(runs);
    
    expected.forEach(i -> actions.add(trailer.begin(new TestTask(completed, i))));
    Apply.to(actions).transform(Collections::shuffle).forEach(a -> a.complete());
    
    wait.until(Size.of(completed).is(runs));
    assertEquals(expected, completed);
  }
  
  private static class Apply<T> {
    private final List<T> list;
    
    private Apply(List<T> list) {
      this.list = list;
    }
    
    static <T> Apply<T> to(List<T> list) {
      return new Apply<>(list);
    }
    
    List<T> transform(Consumer<List<T>> transform) {
      final List<T> copy = new ArrayList<>(list);
      transform.accept(copy);
      return copy;
    }
  }
  
  private static List<Integer> increasingListOf(int numElements) {
    final List<Integer> nums = new ArrayList<>(numElements);
    for (int i = 0; i < numElements; i++) {
      nums.add(i);
    }
    return nums;
  }
  
  private static class Size {
    private final List<?> list;
    
    private Size(List<?> list) {
      this.list = list;
    }
    
    static Size of(List<?> list) {
      return new Size(list);
    }
    
    Runnable is(int numberOfElements) {
      return () -> {
        assertEquals(numberOfElements, list.size());
      };
    }
  }
}
