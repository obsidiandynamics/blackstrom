package com.obsidiandynamics.blackstrom.tracer;

import java.util.concurrent.atomic.*;
import java.util.function.*;

@FunctionalInterface
public interface FiringStrategyFactory extends Function<AtomicReference<Action>, FiringStrategy> {}
