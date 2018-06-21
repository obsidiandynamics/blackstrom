package com.obsidiandynamics.blackstrom.model;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.factor.*;

public final class CommandProcessorTest {
  @Test
  public void testNop() {
    final CommandProcessor proc = mock(CommandProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onCommand(null, null);
  }
}
