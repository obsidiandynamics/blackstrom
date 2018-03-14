package com.obsidiandynamics.blackstrom.hazelcast.elect;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class RegisterTest {
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new Register());
  }
  
  @Test 
  public void testEnrollUnenroll() {
    final Register r = new Register();
    assertEquals(Collections.emptyMap(), r.getCandidatesView());    
    assertNull(r.getRandomCandidate("key"));
    
    final UUID c0 = UUID.randomUUID();
    r.enroll("key", c0);
    assertEquals(1, r.getCandidatesView().size());
    assertEquals(setOf(c0), r.getCandidatesView().get("key"));
    assertNotNull(r.getRandomCandidate("key"));
    
    final UUID c1 = UUID.randomUUID();
    r.enroll("key", c1);
    assertEquals(1, r.getCandidatesView().size());
    assertEquals(setOf(c0, c1), r.getCandidatesView().get("key"));
    assertNotNull(r.getRandomCandidate("key"));
    
    r.unenroll("key", c0);
    assertEquals(1, r.getCandidatesView().size());
    assertEquals(setOf(c1), r.getCandidatesView().get("key"));
    assertNotNull(r.getRandomCandidate("key"));
    
    r.unenroll("key", c1);
    assertEquals(Collections.emptyMap(), r.getCandidatesView());
    assertNull(r.getRandomCandidate("key"));
  }
  
  @SafeVarargs
  private static <T> Set<T> setOf(T... items) {
    return new HashSet<>(Arrays.asList(items));
  }
}
