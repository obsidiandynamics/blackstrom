package com.obsidiandynamics.blackstrom.model;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.factor.*;

public final class CommandResponseProcessorTest {
  @Test
  public void testNop() {
    final CommandResponseProcessor proc = mock(CommandResponseProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onCommandResponse(null, null);
  }
}
