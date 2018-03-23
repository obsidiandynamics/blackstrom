package com.obsidiandynamics.blackstrom.worker;

import java.util.*;
import java.util.stream.*;

public final class Terminator implements Terminable {
  private final Set<Terminable> terminables = new HashSet<>();
  
  private Terminator() {}
  
  public Terminator add(Collection<Terminable> terminables) {
    terminables.forEach(this::add);
    return this;
  }
  
  public Terminator add(Terminable... terminables) {
    return add(Arrays.asList(terminables));
  }
  
  public Terminator add(Terminable terminable) {
    terminables.add(terminable);
    return this;
  }
  
  public Terminator add(Optional<Terminable> terminableOpt) {
    terminableOpt.ifPresent(this::add);
    return this;
  }
  
  public Terminator remove(Terminable terminable) {
    terminables.remove(terminable);
    return this;
  }
  
  public Collection<Terminable> view() {
    return Collections.unmodifiableCollection(new HashSet<>(terminables));
  }
  
  @Override
  public String toString() {
    return terminables.toString();
  }

  @Override
  public Joinable terminate() {
    final List<Joinable> joinables = terminables.stream().map(t -> t.terminate()).collect(Collectors.toList());
    return timeoutMillis -> Joinable.joinAll(timeoutMillis, joinables);
  }
  
  public static Terminator of(Collection<Terminable> terminables) {
    return new Terminator().add(terminables);
  }
  
  public static Terminator of(Terminable... terminables) {
    return new Terminator().add(terminables);
  }
  
  public static Terminator blank() {
    return new Terminator();
  }
}
