package com.obsidiandynamics.blackstrom.ledger;

import java.io.*;
import java.util.function.*;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface AppendCallback {
  static AppendCallback errorLoggingAppendCallback(PrintStream stream) {
    return appendErrorHandler(x -> x.printStackTrace(stream));
  }
  
  static AppendCallback appendErrorHandler(Consumer<Throwable> errorHandler) {
    return (__id, error) -> { 
      if (error != null) errorHandler.accept(error);
    };
  }
  
  static AppendCallback nop() { return (__id, __error) -> {}; }
  
  void onAppend(MessageId messageId, Throwable error);
}
