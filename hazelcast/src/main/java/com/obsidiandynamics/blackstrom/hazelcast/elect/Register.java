package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

public final class Register {
  private final Map<String, Set<UUID>> candidates = new HashMap<>();
  private final Object lock = new Object();
  
  public void enroll(String interestKey, UUID candidateId) {
    synchronized (lock) {
      final Set<UUID> candidatesForKey = candidates.computeIfAbsent(interestKey, k -> new HashSet<>());
      candidatesForKey.add(candidateId);
    }
  }
  
  public void unenroll(String interestKey, UUID candidateId) {
    synchronized (lock) {
      final Set<UUID> candidatesForKey = candidates.getOrDefault(interestKey, Collections.emptySet());
      candidatesForKey.remove(candidateId);
      if (candidatesForKey.isEmpty()) {
        candidates.remove(interestKey);
      }
    }
  }
  
  public Map<String, Set<UUID>> getCandidatesView() {
    final Map<String, Set<UUID>> copy = new HashMap<>();
    synchronized (lock) {
      for (Map.Entry<String, Set<UUID>> entry : candidates.entrySet()) {
        copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
      }
    }
    return Collections.unmodifiableMap(copy);
  }
  
  UUID getRandomCandidate(String interestKey) {
    synchronized (lock) {
      final Set<UUID> candidatesForKey = candidates.getOrDefault(interestKey, Collections.emptySet());
      if (candidatesForKey.isEmpty()) {
        return null;
      } else {
        final int randomIndex = (int) (Math.random() * candidatesForKey.size());
        return new ArrayList<>(candidatesForKey).get(randomIndex);
      }
    }
  }

  @Override
  public String toString() {
    return Register.class.getSimpleName() + " [candidates=" + candidates + "]";
  }
}
