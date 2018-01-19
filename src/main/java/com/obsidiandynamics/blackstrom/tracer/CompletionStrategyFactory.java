package com.obsidiandynamics.blackstrom.tracer;

import java.util.concurrent.atomic.*;
import java.util.function.*;

@FunctionalInterface
public interface CompletionStrategyFactory extends Function<AtomicReference<Action>, CompletionStrategy> {}
