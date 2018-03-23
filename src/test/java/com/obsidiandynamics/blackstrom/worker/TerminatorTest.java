package com.obsidiandynamics.blackstrom.worker;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class TerminatorTest {
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(Terminator.blank());
  }
  
  @Test
  public void testAddRemove() {
    final Terminable t0 = () -> null;
    final Terminable t1 = () -> null;
    final Terminator terminator = Terminator.blank().add(t0, t1);
    final Collection<Terminable> viewAfterAdd = terminator.view();
    assertEquals(2, viewAfterAdd.size());
    assertTrue(viewAfterAdd.contains(t0));
    assertTrue(viewAfterAdd.contains(t1));
    
    terminator.remove(t1);
    final Collection<Terminable> viewAfterRemove = terminator.view();
    assertEquals(1, viewAfterRemove.size());
    assertTrue(viewAfterRemove.contains(t0));
  }
  
  @Test
  public void testTerminateAndJoin() throws InterruptedException {
    final Terminable t = mock(Terminable.class);
    final Joinable j = mock(Joinable.class);
    when(t.terminate()).thenReturn(j);
    final Terminator terminator = Terminator.of(t);

    final Joinable joinable = terminator.terminate();
    verify(t).terminate();
    assertNotNull(joinable);
    
    joinable.joinSilently();
    assertFalse(Thread.interrupted());
    verify(j).join(anyLong());
  }
}
